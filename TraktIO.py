"""
Enhanced TraktIO module with robust rate limiting and retry mechanisms.
Fixes for persistent 429 errors and episode loss issues.
"""

from __future__ import absolute_import, division, print_function

import json
import logging
import os.path
from threading import Condition
import time
from trakt import Trakt
import config
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from requests.exceptions import HTTPError

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
    - Enhanced rate limiting with exponential backoff
    - Comprehensive error tracking and retry logic
    - Dry run mode for testing
    """

    # Rate limiting constants (tuned for Trakt API limits)
    INITIAL_BATCH_DELAY = 3.0  # Increased from 1.0 to prevent initial rate limits
    RATE_LIMIT_DELAY = 30.0  # Delay after 429 error (increased from 15)
    SERVER_ERROR_DELAY = 10.0  # Delay after 5xx error
    MAX_RETRY_ATTEMPTS = 5  # Increased retry attempts for resilience

    def __init__(self, page_size=50, dry_run=False):
        # Configure Trakt client credentials
        Trakt.configuration.defaults.client(
            id=config.TRAKT_API_CLIENT_ID, secret=config.TRAKT_API_CLIENT_SECRET
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

        # Track failed items for retry or reporting
        self._failed_episodes = []
        self._failed_movies = []

        self.dry_run = dry_run
        self.is_authenticating = Condition()
        self.page_size = page_size

        # Rate limiting tracker
        self._last_api_call_time = 0
        self._consecutive_rate_limits = 0

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
                print(
                    "No watched shows found. Token may still be invalid or no data available."
                )
                # Explicitly clear caches for fresh environment
                self._watched_episodes.clear()
                self._watched_movies.clear()

    def cacheWatchedHistory(self):
        """
        Cache all watched episodes and movies to prevent duplicate submissions.
        Handles empty responses properly for fresh Trakt environments.
        """
        try:
            watched_shows = self.getWatchedShows()
            watched_movies = self.getWatchedMovies()

            # Clear existing caches
            self._watched_episodes.clear()
            self._watched_movies.clear()

            # Handle shows - check for None or empty
            if watched_shows:
                # Convert generator to list if needed
                if hasattr(watched_shows, "__iter__"):
                    watched_shows = list(watched_shows)

                if watched_shows:  # Check if list is not empty
                    for show in watched_shows:
                        show_title = show.title if hasattr(show, "title") else str(show)

                        # Cache by Trakt ID if available
                        if hasattr(show, "pk"):
                            for season in show.seasons:
                                for episode in season.episodes:
                                    if hasattr(episode, "watched") and episode.watched:
                                        trakt_id = (
                                            show.pk[0],
                                            show.pk[1],
                                            season.pk,
                                            episode.pk,
                                        )
                                        self._watched_episodes.add(trakt_id)

                        # Also cache by show name/season/episode for redundancy
                        if hasattr(show, "seasons"):
                            for season in show.seasons:
                                season_num = (
                                    season.pk
                                    if hasattr(season, "pk")
                                    else season.number
                                )
                                if hasattr(season, "episodes"):
                                    for episode in season.episodes:
                                        if (
                                            hasattr(episode, "watched")
                                            and episode.watched
                                        ):
                                            ep_num = (
                                                episode.pk
                                                if hasattr(episode, "pk")
                                                else episode.number
                                            )
                                            episode_key = (
                                                show_title.lower(),
                                                season_num,
                                                ep_num,
                                            )
                                            self._watched_episodes.add(episode_key)
                    logging.info(
                        f"Cached {len(self._watched_episodes)} watched episodes"
                    )
                else:
                    logging.info("No watched shows found in Trakt (fresh environment)")
            else:
                logging.info("No watched shows response from Trakt (fresh environment)")

            # Handle movies - check for None or empty
            if watched_movies:
                # Convert generator to list if needed
                if hasattr(watched_movies, "__iter__"):
                    watched_movies = list(watched_movies)

                if watched_movies:  # Check if list is not empty
                    for movie in watched_movies:
                        if hasattr(movie, "ids") and hasattr(movie.ids, "tmdb"):
                            self._watched_movies.add(movie.ids.tmdb)
                    logging.info(f"Cached {len(self._watched_movies)} watched movies")
                else:
                    logging.info("No watched movies found in Trakt (fresh environment)")
            else:
                logging.info(
                    "No watched movies response from Trakt (fresh environment)"
                )

        except Exception as e:
            logging.error(f"Error caching watched history: {e}")
            # Clear caches on error to prevent false positives
            self._watched_episodes.clear()
            self._watched_movies.clear()
            logging.info("Cleared caches due to error - treating as fresh environment")

    def getWatchedShows(self):
        """Retrieve all watched TV shows from Trakt"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                return Trakt["sync/watched"].shows()
        except Exception as e:
            logging.error(f"Error getting watched shows: {e}")
            return None

    def getWatchedMovies(self):
        """Retrieve all watched movies from Trakt"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                return Trakt["sync/watched"].movies()
        except Exception as e:
            logging.error(f"Error getting watched movies: {e}")
            return None

    def isMovieWatched(self, tmdb_id: int) -> bool:
        """Check if a movie (by TMDB ID) is already marked as watched"""
        return tmdb_id in self._watched_movies

    def isEpisodeWatched(
        self, show_name: str, season_number: int, episode_number: int
    ) -> bool:
        """Check if an episode is already marked as watched"""
        if episode_number is None:
            logging.debug(
                f"isEpisodeWatched({show_name}, S{season_number:02d}E??) -> False (episode number unknown)"
            )
            return False

        episode_key = (show_name.lower(), season_number, episode_number)
        result = episode_key in self._watched_episodes
        logging.debug(
            f"isEpisodeWatched({show_name}, S{season_number:02d}E{episode_number:02d}) -> {result}"
        )
        return result

    def addMovie(self, movie_data: dict):
        """Add a movie to the pending sync buffer"""
        self._movies.append(movie_data)

    def addEpisodeToHistory(self, episode_data: dict):
        """Add an episode to the pending sync buffer"""
        self._episodes.append(episode_data)

    def getData(self) -> dict:
        """Get pending sync data"""
        return {"movies": self._movies, "episodes": self._episodes}

    def _enforce_rate_limit(self, min_delay=1.0):
        """
        Enforce minimum delay between API calls to prevent rate limiting.
        Uses adaptive delays based on recent rate limit hits.
        """
        current_time = time.time()
        time_since_last_call = current_time - self._last_api_call_time

        # Use longer delay if we've hit rate limits recently
        if self._consecutive_rate_limits > 0:
            min_delay = max(
                min_delay,
                self.RATE_LIMIT_DELAY * (1 + self._consecutive_rate_limits * 0.5),
            )
            logging.info(
                f"Using extended delay of {min_delay}s due to {self._consecutive_rate_limits} recent rate limits"
            )

        if time_since_last_call < min_delay:
            sleep_time = min_delay - time_since_last_call
            logging.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)

        self._last_api_call_time = time.time()

    def sync(self):
        """
        Perform batch sync to Trakt with enhanced rate limiting and retry logic.
        Syncs movies and episodes in configurable batch sizes.
        """
        if self.dry_run:
            logging.info("Dry run enabled. Skipping actual Trakt sync.")
            return {
                "added": {"movies": len(self._movies), "episodes": len(self._episodes)},
                "not_found": {"movies": [], "episodes": [], "shows": []},
                "updated": {"movies": [], "episodes": []},
                "failed": {"movies": 0, "episodes": 0},
            }

        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                result = {
                    "added": {"movies": 0, "episodes": 0},
                    "not_found": {"movies": [], "episodes": [], "shows": []},
                    "updated": {"movies": [], "episodes": []},
                    "failed": {"movies": 0, "episodes": 0},
                }

                # Get batch delay from config or use enhanced default
                batch_delay = getattr(
                    config, "TRAKT_API_BATCH_DELAY", self.INITIAL_BATCH_DELAY
                )

                # Add initial delay before first API call to prevent immediate rate limit
                logging.info(
                    "Adding initial delay before sync to prevent rate limiting..."
                )
                time.sleep(self.INITIAL_BATCH_DELAY)

                if self._movies:
                    result["added"]["movies"] += self._sync_movies_in_batches(
                        result, batch_delay
                    )

                # Add delay between movies and episodes sync
                if self._movies and self._episodes:
                    logging.info(
                        f"Waiting {batch_delay}s between movies and episodes sync..."
                    )
                    time.sleep(batch_delay)

                if self._episodes:
                    result["added"]["episodes"] += self._sync_episodes_in_batches(
                        result, batch_delay
                    )

                # Log comprehensive results
                logging.info("=== TRAKT SYNC RESULTS ===")
                logging.info(f"Raw response: {result}")
                logging.info(
                    f"Movies - Submitted: {len(self._movies)}, Added: {result['added']['movies']}, "
                    f"Skipped (duplicates): {len(self._movies) - result['added']['movies'] - result['failed']['movies']}, "
                    f"Failed (API errors): {result['failed']['movies']}"
                )
                logging.info(
                    f"Episodes - Submitted: {len(self._episodes)}, Added: {result['added']['episodes']}, "
                    f"Skipped (duplicates): {len(self._episodes) - result['added']['episodes'] - result['failed']['episodes']}, "
                    f"Failed (API errors): {result['failed']['episodes']}"
                )

                if result["failed"]["episodes"] > 0:
                    logging.error(
                        f"CRITICAL: {result['failed']['episodes']} episodes were LOST due to persistent API failures!"
                    )

                return result

        except Exception as e:
            logging.error(f"Trakt sync failed: {e}")
            raise

    def _sync_movies_in_batches(self, result: dict, batch_delay: float) -> int:
        """Sync queued movie history entries in batches with enhanced retry logic"""
        added_total = 0
        total = len(self._movies)
        total_batches = ((total - 1) // self.page_size + 1) if total > 0 else 0

        logging.info(f"Syncing {total} movies in {total_batches} batches of {self.page_size}")

        for i in range(0, total, self.page_size):
            batch = self._movies[i : i + self.page_size]
            batch_num = i // self.page_size + 1

            # Enforce rate limit before each batch
            self._enforce_rate_limit(batch_delay)

            try:
                response = self._sync_batch_with_retry(
                    {"movies": batch}, "movies", batch_num, batch_delay
                )
                if response:
                    added = response.get("added", {}).get("movies", 0)
                    added_total += added

                    # Reset consecutive rate limit counter on success
                    self._consecutive_rate_limits = 0

                    logging.info(
                        f"Movie batch {batch_num}: Added {added}/{len(batch)} movies"
                    )

                    # Process not_found and updated items
                    if "not_found" in response:
                        nf_movies = response["not_found"].get("movies", [])
                        if isinstance(nf_movies, list):
                            result["not_found"]["movies"].extend(nf_movies)
                    if "updated" in response:
                        upd_movies = response["updated"].get("movies", [])
                        if isinstance(upd_movies, list):
                            result["updated"]["movies"].extend(upd_movies)
                else:
                    # No response received after all retries
                    logging.error(
                        f"Movie batch {batch_num}: No response received from Trakt API"
                    )
                    result["failed"]["movies"] += len(batch)
                    self._failed_movies.extend(batch)

            except Exception as e:
                logging.error(
                    f"Movie batch {batch_num} failed permanently after all retries: {e}"
                )
                result["failed"]["movies"] += len(batch)
                self._failed_movies.extend(batch)

        return added_total

    def _sync_episodes_in_batches(self, result: dict, batch_delay: float) -> int:
        """Sync queued episode history entries in batches with enhanced retry logic"""
        added_total = 0
        total = len(self._episodes)
        total_batches = ((total - 1) // self.page_size + 1) if total > 0 else 0

        logging.info(
            f"Syncing {total} episodes in {total_batches} batches of {self.page_size}"
        )

        for i in range(0, total, self.page_size):
            batch = self._episodes[i : i + self.page_size]
            batch_num = i // self.page_size + 1

            logging.info(
                f"Processing episode batch {batch_num}/{total_batches}: {len(batch)} episodes"
            )

            # Enforce rate limit before each batch
            self._enforce_rate_limit(batch_delay)

            try:
                response = self._sync_batch_with_retry(
                    {"episodes": batch}, "episodes", batch_num, batch_delay
                )
                if response:
                    added = response.get("added", {}).get("episodes", 0)
                    added_total += added

                    # Reset consecutive rate limit counter on success
                    self._consecutive_rate_limits = 0

                    # Enhanced logging for batch results
                    batch_failed = len(batch) - added
                    if batch_failed > 0:
                        logging.warning(
                            f"Episode batch {batch_num}: Added {added}/{len(batch)} episodes, "
                            f"{batch_failed} not added"
                        )
                    else:
                        logging.info(
                            f"Episode batch {batch_num}: Added {added}/{len(batch)} episodes "
                            f"(100% success)"
                        )

                    # Process not_found items
                    if "not_found" in response:
                        nf_eps = response["not_found"].get("episodes", [])
                        if isinstance(nf_eps, list):
                            result["not_found"]["episodes"].extend(nf_eps)

                    # Process updated items
                    if "updated" in response:
                        upd_eps = response["updated"].get("episodes", [])
                        if isinstance(upd_eps, list):
                            result["updated"]["episodes"].extend(upd_eps)
                else:
                    # No response received after all retries
                    logging.error(
                        f"Episode batch {batch_num}: No response received from Trakt API"
                    )
                    logging.error(
                        f"LOST EPISODES: {len(batch)} episodes failed due to no API response "
                        f"in batch {batch_num}"
                    )
                    result["failed"]["episodes"] += len(batch)
                    self._failed_episodes.extend(batch)

            except Exception as e:
                logging.error(
                    f"Episode batch {batch_num} failed permanently after all retries: {e}"
                )
                result["failed"]["episodes"] += len(batch)
                logging.error(
                    f"LOST EPISODES: {len(batch)} episodes failed due to persistent API errors "
                    f"in batch {batch_num}"
                )
                # Log details of failed episodes for debugging
                logging.debug(
                    f"Failed episode batch {batch_num} contained TMDB IDs: "
                    f"{[ep.get('ids', {}).get('tmdb') for ep in batch]}"
                )
                self._failed_episodes.extend(batch)

        # Final validation
        logging.info(
            f"Episode batch sync complete: {added_total}/{total} episodes successfully added"
        )
        if result["failed"]["episodes"] > 0:
            logging.error(
                f"CRITICAL: {result['failed']['episodes']} episodes were LOST during sync"
            )

        return added_total

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type((HTTPError, Exception)),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )
    def _sync_batch_with_retry(self, data: dict, content_type: str, batch_num: int, batch_delay: float = 3.0):
        """
        Sync a single batch with enhanced exponential backoff retry for rate limits.
        Returns None if all retries are exhausted.
        """
        try:
            response = Trakt["sync/history"].add(data)

            # Check if we got an actual response
            if response is None:
                logging.warning(
                    f"Batch {batch_num}: Received None response from Trakt API"
                )
                raise Exception("No response from Trakt API")

            return response

        except Exception as e:
            error_str = str(e).lower()

            # Track and handle rate limits
            if "429" in str(e) or "rate" in error_str:
                self._consecutive_rate_limits += 1
                logging.warning(
                    f"RATE LIMIT: 429 error during {content_type} batch {batch_num} sync: {e}"
                )
                logging.info(
                    f"Consecutive rate limits: {self._consecutive_rate_limits}"
                )
                # Use longer delay for rate limits
                time.sleep(self.RATE_LIMIT_DELAY)

            elif (
                "500" in str(e)
                or "502" in str(e)
                or "503" in str(e)
                or "server" in error_str
            ):
                logging.warning(
                    f"SERVER ERROR: 5xx error during {content_type} batch {batch_num} sync: {e}"
                )
                time.sleep(self.SERVER_ERROR_DELAY)

            else:
                logging.warning(
                    f"API ERROR: Unexpected error during {content_type} batch {batch_num} sync: {e}"
                )
                time.sleep(batch_delay if "batch_delay" in locals() else 3.0)

            raise e

    def get_failed_items(self):
        """Return lists of failed movies and episodes for potential retry or logging"""
        return {"movies": self._failed_movies, "episodes": self._failed_episodes}

    def _on_token_refreshed(self, authorization):
        """Handle token refresh events from trakt.py"""
        self.authorization = authorization
        with open("traktAuth.json", "w") as f:
            json.dump(self.authorization, f)
        logging.info("Trakt token refreshed and saved")

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
