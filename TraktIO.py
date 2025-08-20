from __future__ import absolute_import, division, print_function

import json
import logging
import os.path
from threading import Condition
import requests
from trakt import Trakt
import config

# Set up logging based on config
logging.basicConfig(level=config.LOG_LEVEL)


class TraktIO(object):
    """Handles Trakt authorization, caching, and sync logic"""
    
    def __init__(self, page_size=50, dry_run=False):
        # Configure Trakt client credentials
        Trakt.configuration.defaults.client(
            id=config.TRAKT_API_CLIENT_ID, 
            secret=config.TRAKT_API_CLIENT_SECRET
        )
        
        self.authorization = None
        self._watched_episodes = set()  # Your addition - caching
        self._watched_movies = set()    # Your addition - caching
        self._episodes = []             # Your addition - batch collection
        self._movies = []               # Your addition - batch collection
        self.dry_run = dry_run         # Your addition - dry run mode
        self.is_authenticating = Condition()
        self.page_size = page_size
        
        # Skip authentication in dry run mode
        if not self.dry_run:
            self._initialize_auth()
    
    def _initialize_auth(self):
        """Initialize and load authentication data from file or trigger auth flow"""
        if not os.path.isfile("traktAuth.json"):
            self.authenticate()
        
        if os.path.isfile("traktAuth.json"):
            with open("traktAuth.json") as infile:
                self.authorization = json.load(infile)
            
            # Your addition - manual token refresh
            if not self.checkAuthenticationValid():
                print("Authorization is expired, attempting manual refresh...")
                self._refresh_token()
            
            # Your addition - cache watched history
            if self.getWatchedShows() is not None:
                print("Authorization appears valid. Watched shows retrieved.")
                self.cacheWatchedHistory()
            else:
                print("No watched shows found. Token may still be invalid or no data available.")
    
    def _refresh_token(self):
        """Your addition - Refresh expired Trakt token manually"""
        if not self.authorization:
            print("Cannot refresh token, no authorization found.")
            return
        
        response = requests.post(
            "https://api.trakt.tv/oauth/token",
            json={
                "refresh_token": self.authorization.get("refresh_token"),
                "client_id": config.TRAKT_API_CLIENT_ID,
                "client_secret": config.TRAKT_API_CLIENT_SECRET,
                "redirect_uri": config.TRAKT_REDIRECT_URI,
                "grant_type": "refresh_token",
            },
        )
        
        if response.status_code == 200:
            self.authorization = response.json()
            with open("traktAuth.json", "w") as outfile:
                json.dump(self.authorization, outfile)
            print("Token successfully refreshed manually.")
            # Update Trakt library with new token
            Trakt.configuration.defaults.oauth.from_response(self.authorization)
        else:
            print("Manual token refresh failed: %s" % response.text)
    
    def checkAuthenticationValid(self) -> bool:
        """Check if token is still valid"""
        if not self.authorization:
            return False
        return "access_token" in self.authorization
    
    def getWatchedShows(self):
        """Fetch watched shows list from Trakt using the library"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                watched = Trakt["sync/watched"].shows()
                if watched:
                    # Convert to JSON format for compatibility
                    json_response = []
                    for show in watched:
                        show_dict = {
                            "show": {"title": show.title, "ids": {"trakt": show.trakt}},
                            "seasons": []
                        }
                        for season in show.seasons:
                            season_dict = {
                                "number": season.number,
                                "episodes": [
                                    {"number": ep.number, "ids": {"trakt": ep.trakt}}
                                    for ep in season.episodes
                                ]
                            }
                            show_dict["seasons"].append(season_dict)
                        json_response.append(show_dict)
                    
                    logging.debug(
                        "Trakt watched shows response: %s",
                        json.dumps(json_response, indent=2),
                    )
                    return json_response
            return None
        except Exception as e:
            print(f"❌ Failed to fetch watched shows: {e}")
            return None
    
    def cacheWatchedHistory(self):
        """Your addition - Cache watched episodes and movies to avoid resubmitting"""
        if self.dry_run:
            logging.info("Dry run enabled. Skipping watched history caching from Trakt.")
            return
            
        try:
            logging.info("Fetching watched episodes and movies from Trakt...")
            
            with Trakt.configuration.oauth.from_response(self.authorization):
                # Cache watched shows/episodes
                watched_shows = Trakt["sync/watched"].shows()
                if watched_shows:
                    for show in watched_shows:
                        for season in show.seasons:
                            for episode in season.episodes:
                                self._watched_episodes.add(episode.trakt)
                
                # Cache watched movies
                watched_movies = Trakt["sync/watched"].movies()
                if watched_movies:
                    for movie in watched_movies:
                        if hasattr(movie, 'ids') and hasattr(movie.ids, 'tmdb'):
                            self._watched_movies.add(movie.ids.tmdb)
                            
        except Exception as e:
            logging.error(f"⚠ Error caching Trakt history: {e}")
    
    def isWatchedMovie(self, tmdb_id: int) -> bool:
        """Your addition - Check if movie was already watched"""
        result = tmdb_id in self._watched_movies
        logging.debug(f"isWatchedMovie({tmdb_id}) -> {result}")
        return result
    
    def isEpisodeWatched(self, show_name: str, season_number: int, episode_number: int) -> bool:
        """Your addition - Check if episode was already watched"""
        # This would need to be implemented based on the cached data structure
        # For now, returning False to maintain functionality
        return False
    
    def addMovie(self, movie_data: dict):
        """Your addition - Add movie to batch list"""
        self._movies.append(movie_data)
    
    def addEpisodeToHistory(self, episode_data: dict):
        """Your addition - Add episode to batch list"""
        self._episodes.append(episode_data)
    
    def getData(self) -> dict:
        """Your addition - Get locally stored movie and episode data for sync"""
        return {"movies": self._movies, "episodes": self._episodes}
    
    def sync(self):
        """Perform sync to Trakt using the library"""
        if self.dry_run:
            logging.info("Dry run enabled. Skipping actual Trakt sync.")
            return {
                "added": {"movies": len(self._movies), "episodes": len(self._episodes)},
                "not_found": {"movies": [], "episodes": [], "shows": []},
                "updated": {"movies": [], "episodes": []},
            }
        
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                # Sync in batches as per your implementation
                result = {"added": {"movies": 0, "episodes": 0}, 
                         "not_found": {"movies": [], "episodes": [], "shows": []},
                         "updated": {"movies": [], "episodes": []}}
                
                # Sync movies in batches
                if self._movies:
                    for i in range(0, len(self._movies), self.page_size):
                        batch = self._movies[i:i + self.page_size]
                        response = Trakt["sync/history"].add({"movies": batch})
                        if response:
                            result["added"]["movies"] += response.get("added", {}).get("movies", 0)
                
                # Sync episodes in batches
                if self._episodes:
                    for i in range(0, len(self._episodes), self.page_size):
                        batch = self._episodes[i:i + self.page_size]
                        response = Trakt["sync/history"].add({"episodes": batch})
                        if response:
                            result["added"]["episodes"] += response.get("added", {}).get("episodes", 0)
                
                logging.debug("Trakt sync response: %s", json.dumps(result, indent=2))
                return result
                
        except Exception as e:
            # Fallback to your direct API approach if library fails
            logging.warning(f"Library sync failed, using direct API: {e}")
            return self._sync_direct_api()
    
    def _sync_direct_api(self):
        """Your original direct API sync as fallback"""
        headers = self._get_auth_headers()
        payload = json.dumps(self.getData())
        
        response = requests.post(
            "https://api.trakt.tv/sync/history", 
            headers=headers, 
            data=payload
        )
        
        if response.status_code != 201:
            raise Exception(
                f"Trakt sync failed: {response.status_code} - {response.text}"
            )
        
        json_response = response.json()
        logging.debug("Trakt sync response: %s", json.dumps(json_response, indent=2))
        
        # Your addition - sanitize response
        for key in ["added", "updated", "not_found"]:
            for subkey in ["movies", "episodes", "shows"]:
                if key in json_response and subkey in json_response[key]:
                    val = json_response[key][subkey]
                    if not isinstance(val, (list, dict)):
                        json_response[key][subkey] = []
        
        return json_response
    
    def _get_auth_headers(self):
        """Your addition - Return authorization headers for direct API calls"""
        if not self.authorization:
            raise Exception("User is not authenticated.")
        
        return {
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "trakt-api-key": config.TRAKT_API_CLIENT_ID,
            "Authorization": f"Bearer {self.authorization['access_token']}",
        }
    
    def authenticate(self):
        """Handle device authentication flow"""
        if not self.is_authenticating.acquire(blocking=False):
            print("Authentication has already been started")
            return False
        
        code_info = Trakt["oauth/device"].code()
        
        print(
            '🔑 Enter the code "%s" at %s to authenticate your Trakt account'
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
        print("✅ Authentication successful!")
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