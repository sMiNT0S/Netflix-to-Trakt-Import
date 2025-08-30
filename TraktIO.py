from __future__ import absolute_import, division, print_function

import json
import logging
import os.path
from threading import Condition
import time
from trakt import Trakt
import config

# Set up logging based on config
logging.basicConfig(level=config.LOG_LEVEL)


class TraktIO(object):
    """
    Handles Trakt authorization, caching, and sync logic.
    
    Features:
    - OAuth device code authentication flow
    - Automatic token refresh via trakt.py library
    - Caching of watched content to prevent duplicates
    - Batch syncing with configurable page size
    - Dry run mode for testing
    """
    
    def __init__(self, page_size=50, dry_run=False):
        # Configure Trakt client credentials
        Trakt.configuration.defaults.client(
            id=config.TRAKT_API_CLIENT_ID,
            secret=config.TRAKT_API_CLIENT_SECRET
        )

        self.authorization = None
        
        # Caches for preventing duplicate submissions:
        # - _watched_episodes: stores both trakt IDs and (show, season, episode) tuples
        # - _watched_movies: stores TMDB IDs of watched movies
        self._watched_episodes = set()
        self._watched_movies = set()
        
        # Buffers for batch syncing:
        # - _episodes: episode history entries pending sync
        # - _movies: movie history entries pending sync
        self._episodes = []
        self._movies = []
        
        self.dry_run = dry_run
        self.is_authenticating = Condition()
        self.page_size = page_size
        
        # Skip authentication in dry run mode
        if not self.dry_run:
            # Register token refresh handler to persist updated tokens
            Trakt.on("oauth.token_refreshed", self._on_token_refreshed)
            self._initialize_auth()
    
    def _initialize_auth(self):
        """Initialize and load authentication data from file or trigger auth flow"""
        if not os.path.isfile("traktAuth.json"):
            self.authenticate()
        
        if os.path.isfile("traktAuth.json"):
            with open("traktAuth.json") as infile:
                self.authorization = json.load(infile)
            
            # Set the token in trakt.py for automatic refresh
            if self.authorization:
                with Trakt.configuration.oauth.from_response(self.authorization):
                    # This context manager sets up the token for the library
                    pass
            
            if self.getWatchedShows() is not None:
                print("Authorization appears valid. Watched shows retrieved.")
                self.cacheWatchedHistory()
            else:
                print("No watched shows found. Token may still be invalid or no data available.")
    
    def _on_token_refreshed(self, authorization):
        """
        Event callback from trakt.py when token auto-refreshes.
        Persists the refreshed token to disk.
        """
        self.authorization = authorization
        try:
            with open("traktAuth.json", "w") as f:
                json.dump(self.authorization, f)
            logging.debug("Persisted refreshed Trakt token.")
        except Exception as e:
            logging.warning(f"Failed to persist refreshed token: {e}")
    
    def checkAuthenticationValid(self) -> bool:
        """Check if token exists and has required fields"""
        if not self.authorization:
            return False
        
        # Check for essential OAuth fields
        has_token = "access_token" in self.authorization
        
        # Log token info for debugging (without exposing sensitive data)
        if has_token:
            created_at = self.authorization.get("created_at", "unknown")
            expires_in = self.authorization.get("expires_in", "unknown")
            logging.debug(f"Token created_at: {created_at}, expires_in: {expires_in}")
        
        return has_token
    
    def getWatchedShows(self):
        """Fetch watched shows list from Trakt using the library"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                watched = Trakt["sync/watched"].shows()
                if not watched:
                    return None
                
                # Convert generator to list to check if empty
                watched_list = list(watched)
                if not watched_list:
                    return None
                
                # Convert to JSON-compatible format for compatibility
                json_response = []
                for entry in watched_list:
                    # Handle different return formats from trakt.py
                    # The library returns (show, seasons) tuples
                    if isinstance(entry, tuple) and len(entry) == 2:
                        show, seasons = entry
                    else:
                        show = entry
                        seasons = getattr(entry, 'seasons', [])
                    
                    # Build normalized structure
                    show_dict = {
                        "show": {
                            "title": getattr(show, 'title', 'Unknown'),
                            "ids": {"trakt": getattr(show, 'trakt', None)}
                        },
                        "seasons": []
                    }
                    
                    for season in seasons:
                        season_dict = {
                            "number": getattr(season, 'number', 0),
                            "episodes": []
                        }
                        
                        episodes = getattr(season, 'episodes', [])
                        for ep in episodes:
                            season_dict["episodes"].append({
                                "number": getattr(ep, 'number', 0),
                                "ids": {"trakt": getattr(ep, 'trakt', None)}
                            })
                        
                        show_dict["seasons"].append(season_dict)
                    
                    json_response.append(show_dict)
                
                logging.debug(
                    "Trakt watched shows response: %d shows",
                    len(json_response)
                )
                return json_response
                
        except Exception as e:
            logging.error(f"Failed to fetch watched shows: {e}")
            return None
    
    def cacheWatchedHistory(self):
        """Populate watched caches to prevent duplicate submissions."""
        if self.dry_run:
            logging.info("Dry run enabled. Skipping watched history caching from Trakt.")
            return
            
        try:
            logging.info("Fetching watched episodes and movies from Trakt...")
            
            with Trakt.configuration.oauth.from_response(self.authorization):
                # Cache watched shows/episodes
                watched_shows = Trakt["sync/watched"].shows()
                if watched_shows:
                    for entry in watched_shows:
                        # Handle tuple format from trakt.py
                        if isinstance(entry, tuple) and len(entry) == 2:
                            show, seasons = entry
                        else:
                            show = entry
                            seasons = getattr(entry, 'seasons', [])
                        
                        show_name = getattr(show, 'title', '')
                        show_name_lower = show_name.lower() if show_name else ""
                        
                        for season in seasons:
                            season_number = getattr(season, 'number', None)
                            if season_number is None:
                                continue
                                
                            episodes = getattr(season, 'episodes', [])
                            for episode in episodes:
                                # Cache episodes by trakt ID
                                ep_trakt = getattr(episode, 'trakt', None)
                                if ep_trakt:
                                    self._watched_episodes.add(ep_trakt)
                                
                                # Also cache by show name, season, episode for lookup
                                ep_number = getattr(episode, 'number', None)
                                if show_name_lower and ep_number is not None:
                                    episode_key = (show_name_lower, season_number, ep_number)
                                    self._watched_episodes.add(episode_key)
                
                # Cache watched movies
                watched_movies = Trakt["sync/watched"].movies()
                if watched_movies:
                    for entry in watched_movies:
                        # Handle tuple format
                        if isinstance(entry, tuple) and entry:
                            movie = entry[0]
                        else:
                            movie = entry
                        
                        # Get TMDB ID from movie
                        ids = getattr(movie, 'ids', None)
                        if ids:
                            tmdb_id = getattr(ids, 'tmdb', None)
                            if tmdb_id:
                                self._watched_movies.add(tmdb_id)
            
            logging.info(f"Cached {len(self._watched_episodes)} watched episode entries "
                        f"and {len(self._watched_movies)} watched movies")
                            
        except Exception as e:
            logging.error(f"Error caching Trakt history: {e}")
    
    def isWatchedMovie(self, tmdb_id: int) -> bool:
        """Check if a movie with given TMDB ID is already watched."""
        result = tmdb_id in self._watched_movies
        logging.debug(f"isWatchedMovie({tmdb_id}) -> {result}")
        return result
    
    def isEpisodeWatched(self, show_name: str, season_number: int, episode_number: int) -> bool:
        """
        Check if an episode is already watched.
        
        Args:
            show_name: Name of the TV show
            season_number: Season number
            episode_number: Episode number (can be None)
            
        Returns:
            True if episode is in watched cache, False otherwise
        """
        if episode_number is None:
            logging.debug(f"isEpisodeWatched({show_name}, S{season_number:02d}E??) -> False (episode number unknown)")
            return False
        
        episode_key = (show_name.lower(), season_number, episode_number)
        result = episode_key in self._watched_episodes
        logging.debug(
            f"isEpisodeWatched({show_name}, S{season_number:02d}E{episode_number:02d}) -> {result}"
        )
        return result
    
    def addMovie(self, movie_data: dict):
        """Add a movie to the pending sync buffer."""
        self._movies.append(movie_data)
    
    def addEpisodeToHistory(self, episode_data: dict):
        """Add an episode to the pending sync buffer."""
        self._episodes.append(episode_data)
    
    def getData(self) -> dict:
        """Get pending sync data."""
        return {"movies": self._movies, "episodes": self._episodes}
    
    def sync(self):
        """
        Perform batch sync to Trakt using the trakt.py library.
        Syncs movies and episodes in configurable batch sizes.
        """
        if self.dry_run:
            logging.info("Dry run enabled. Skipping actual Trakt sync.")
            return {
                "added": {"movies": len(self._movies), "episodes": len(self._episodes)},
                "not_found": {"movies": [], "episodes": [], "shows": []},
                "updated": {"movies": [], "episodes": []},
            }
        
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                result = {
                    "added": {"movies": 0, "episodes": 0},
                    "not_found": {"movies": [], "episodes": [], "shows": []},
                    "updated": {"movies": [], "episodes": []},
                }

                batch_delay = getattr(config, "TRAKT_API_BATCH_DELAY", 1)

                if self._movies:
                    result["added"]["movies"] += self._sync_movies_in_batches(result, batch_delay)
                if self._episodes:
                    result["added"]["episodes"] += self._sync_episodes_in_batches(result, batch_delay)

                logging.debug("Trakt sync response: %s", json.dumps(result, indent=2))
                return result
                
        except Exception as e:
            logging.error(f"Trakt sync failed: {e}")
            raise
    
    def authenticate(self):
        """Handle device authentication flow"""
        if not self.is_authenticating.acquire(blocking=False):
            print("Authentication has already been started")
            return False
        
        code_info = Trakt["oauth/device"].code()
        
        print(
            'Enter the code "%s" at %s to authenticate your Trakt account'
            % (code_info.get("user_code"), code_info.get("verification_url"))
        )
        
        poller = (
            Trakt["oauth/device"]
            .poll(**code_info)
            .on("aborted", self.on_aborted)
            .on("authenticated", self.on_authenticated)
            .on("expired", self.on_expired)
            .on("poll", self.on_poll)
        )
        
        poller.start(daemon=False)
        return self.is_authenticating.wait()
    
    def on_aborted(self):
        """Called when user aborts Trakt auth"""
        print("Authentication aborted")
        self._notify_auth_complete()
    
    def on_authenticated(self, authorization):
        """Called when user completes authentication successfully"""
        self.authorization = authorization
        print("Authentication successful!")
        with open("traktAuth.json", "w") as f:
            json.dump(self.authorization, f)
        self._notify_auth_complete()
    
    def on_expired(self):
        """Called when auth times out or expires"""
        print("Authentication expired")
        self._notify_auth_complete()
    
    def on_poll(self, callback):
        """Called on every poll attempt during auth"""
        callback(True)
    
    def _notify_auth_complete(self):
        """Notify any threads waiting on authentication that it is complete"""
        self.is_authenticating.acquire()
        self.is_authenticating.notify_all()
        self.is_authenticating.release()

    # --- Internal batch sync helpers ---
    def _sync_movies_in_batches(self, result: dict, batch_delay: float) -> int:
        """Sync queued movie history entries in batches. Updates result in-place and returns added count."""
        added_total = 0
        total = len(self._movies)
        logging.info(f"Syncing {total} movies in batches of {self.page_size}")
        for i in range(0, total, self.page_size):
            batch = self._movies[i : i + self.page_size]
            try:
                response = Trakt["sync/history"].add({"movies": batch})
                if response:
                    added = response.get("added", {}).get("movies", 0)
                    added_total += added
                    logging.debug(f"Movie batch {i // self.page_size + 1}: Added {added} movies")
                    if "not_found" in response:
                        nf_movies = response["not_found"].get("movies", [])
                        if isinstance(nf_movies, list):
                            result["not_found"]["movies"].extend(nf_movies)
                    if "updated" in response:
                        upd_movies = response["updated"].get("movies", [])
                        if isinstance(upd_movies, list):
                            result["updated"]["movies"].extend(upd_movies)
            except Exception as e:
                logging.warning(f"Movie batch {i // self.page_size + 1} failed: {e}")
                continue
            if i + self.page_size < total:
                logging.debug(f"Rate limiting: waiting {batch_delay}s between movie batches")
                time.sleep(batch_delay)
        return added_total

    def _sync_episodes_in_batches(self, result: dict, batch_delay: float) -> int:
        """Sync queued episode history entries in batches. Updates result in-place and returns added count."""
        added_total = 0
        total = len(self._episodes)
        logging.info(f"Syncing {total} episodes in batches of {self.page_size}")
        for i in range(0, total, self.page_size):
            batch = self._episodes[i : i + self.page_size]
            try:
                response = Trakt["sync/history"].add({"episodes": batch})
                if response:
                    added = response.get("added", {}).get("episodes", 0)
                    added_total += added
                    logging.debug(f"Episode batch {i // self.page_size + 1}: Added {added} episodes")
                    if "not_found" in response:
                        nf_eps = response["not_found"].get("episodes", [])
                        if isinstance(nf_eps, list):
                            result["not_found"]["episodes"].extend(nf_eps)
                        nf_shows = response["not_found"].get("shows", [])
                        if isinstance(nf_shows, list):
                            result["not_found"]["shows"].extend(nf_shows)
                    if "updated" in response:
                        upd_eps = response["updated"].get("episodes", [])
                        if isinstance(upd_eps, list):
                            result["updated"]["episodes"].extend(upd_eps)
            except Exception as e:
                logging.warning(f"Episode batch {i // self.page_size + 1} failed: {e}")
                continue
            if i + self.page_size < total:
                logging.debug(f"Rate limiting: waiting {batch_delay}s between episode batches")
                time.sleep(batch_delay)
        return added_total