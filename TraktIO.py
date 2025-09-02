"""
Enhanced TraktIO module with robust rate limiting and retry mechanisms.
Fixes for persistent 429 errors and episode loss issues.
"""

from typing import Optional

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
from requests.exceptions import HTTPError  # type: ignore[import]

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
    - Consistent logging with optional verbose user messages
    """

    # Rate limiting constants (tuned for Trakt API limits)
    INITIAL_BATCH_DELAY = 3.0  # Increased from 1.0 to prevent initial rate limits
    RATE_LIMIT_DELAY = 30.0  # Delay after 429 error (increased from 15)
    SERVER_ERROR_DELAY = 10.0  # Delay after 5xx error
    MAX_RETRY_ATTEMPTS = 5  # Increased retry attempts for resilience

    def __init__(self, page_size=None, dry_run=None, verbose=None):
        # Configure Trakt client credentials
        Trakt.configuration.defaults.client(
            id=config.TRAKT_API_CLIENT_ID, secret=config.TRAKT_API_CLIENT_SECRET
        )

        self.authorization = None

        # Use config defaults if not explicitly provided
        self.page_size = page_size if page_size is not None else config.TRAKT_API_SYNC_PAGE_SIZE
        self.dry_run = dry_run if dry_run is not None else config.TRAKT_API_DRY_RUN
        self.verbose = verbose if verbose is not None else config.TRAKT_API_VERBOSE

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
        
        # Track consecutive authentication failures for fail-fast
        self._consecutive_auth_failures = 0
        self._max_auth_failures = 3  # Fail fast after 3 consecutive auth issues

        self.is_authenticating = Condition()

        # Rate limiting tracker
        self._last_api_call_time = 0
        self._consecutive_rate_limits = 0

        # Skip authentication in dry run mode
        if not self.dry_run:
            # Register token refresh handler to persist updated tokens
            Trakt.on("oauth.token_refreshed", self._on_token_refreshed)
            self._initialize_auth()

    def _user_message(self, message: str, level: str = "info"):
        """
        Output user-facing messages through logging with optional verbosity control.
        
        Args:
            message: The message to display
            level: Logging level ('info', 'warning', 'error', 'critical')
        """
        # Always show critical messages and authentication-related warnings directly to console
        if (level in ["critical", "error"] or 
            ("authenticat" in message.lower() or "token" in message.lower() or "watched shows" in message.lower() or 
             "trakt.tv" in message.lower() or "batch" in message.lower() or "processing" in message.lower())):
            print(f"{message}")
        
        # Also log through the logging system
        if self.verbose:
            if level == "info":
                logging.info(f"USER: {message}")
            elif level == "warning":
                logging.warning(f"USER: {message}")
            elif level == "error":
                logging.error(f"USER: {message}")
            elif level == "critical":
                logging.critical(f"USER: {message}")
        else:
            # Always log at debug level for troubleshooting
            logging.debug(f"USER ({level.upper()}): {message}")

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
                self._user_message("Authorization appears valid. Watched shows retrieved.", "info")
                self.cacheWatchedHistory()
            else:
                self._user_message(
                    "No watched shows found. Token validation passed but empty response received. For new accounts this is expected. For existing accounts with history, consider token refresh or re-authentication.",
                    "warning"
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

            # DEBUG: Add comprehensive logging to debug cache count discrepancy
            movie_count = "unknown"
            try:
                if watched_movies:
                    if hasattr(watched_movies, '__len__'):
                        movie_count = len(watched_movies)
                    else:
                        # Convert generator to list to get count
                        watched_movies_list = list(watched_movies)
                        movie_count = len(watched_movies_list)
                        watched_movies = watched_movies_list  # Use the list version
                else:
                    movie_count = 0
            except Exception as count_error:
                logging.debug(f"Error getting movie count: {count_error}")
                movie_count = "error"
            logging.info(f"DEBUG: Movie API response type: {type(watched_movies)}, length: {movie_count}")
            logging.info("=== CACHE DEBUGGING: Starting watched history analysis ===")

            # Handle shows - check for None or empty
            if watched_shows:
                # Convert generator to list if needed
                if hasattr(watched_shows, "__iter__"):
                    watched_shows = list(watched_shows)

                if watched_shows:  # Check if list is not empty
                    logging.info(f"DEBUG: Processing {len(watched_shows)} watched shows from API")
                    
                    total_seasons = 0
                    total_api_episodes = 0
                    total_watched_episodes = 0
                    id_based_adds = 0
                    name_based_adds = 0
                    
                    for show_index, show in enumerate(watched_shows):
                        show_title = show.title if hasattr(show, "title") else str(show)
                        
                        if show_index < 3:  # Log first 3 shows for debugging
                            logging.info(f"DEBUG: Show {show_index + 1}: '{show_title}' - {len(show.seasons) if hasattr(show, 'seasons') else 0} seasons")

                        # Cache by Trakt ID if available
                        if hasattr(show, "pk"):
                            # Seasons and episodes are dictionaries keyed by number
                            if hasattr(show, "seasons") and show.seasons:
                                total_seasons += len(show.seasons)
                                
                                for season_num, season in show.seasons.items():
                                    if hasattr(season, "episodes") and season.episodes:
                                        total_api_episodes += len(season.episodes)
                                        
                                        for episode_num, episode in season.episodes.items():
                                            # Check if episode was watched using proper attributes
                                            if (hasattr(episode, "last_watched_at") and episode.last_watched_at) or \
                                               (hasattr(episode, "plays") and episode.plays and episode.plays > 0):
                                                total_watched_episodes += 1
                                                
                                                # Use name-based caching for consistent lookup format
                                                # This ensures cache format matches isEpisodeWatched() lookup format
                                                episode_key = (
                                                    show_title.lower(),
                                                    season_num,
                                                    episode_num,
                                                )
                                                self._watched_episodes.add(episode_key)
                                                name_based_adds += 1

                    # DEBUG: Log detailed statistics
                    logging.info("=== CACHE DEBUG STATISTICS ===")
                    logging.info(f"Shows processed: {len(watched_shows)}")
                    logging.info(f"Total seasons: {total_seasons}")
                    logging.info(f"Total episodes from API: {total_api_episodes}")
                    logging.info(f"Episodes with watch data: {total_watched_episodes}")
                    logging.info(f"ID-based cache additions: {id_based_adds}")
                    logging.info(f"Name-based cache additions: {name_based_adds}")
                    logging.info(f"Final cache size: {len(self._watched_episodes)}")
                    logging.info(f"Cache efficiency: {len(self._watched_episodes)} / ({id_based_adds} + {name_based_adds}) = {len(self._watched_episodes) / (id_based_adds + name_based_adds) * 100:.1f}%")
                    
                    # Show sample cache entries
                    if self._watched_episodes:
                        sample_episodes = list(self._watched_episodes)[:10]
                        logging.info(f"Sample cache entries (first 10): {sample_episodes}")

                    logging.info(
                        f"Cached {len(self._watched_episodes)} watched episodes"
                    )
                else:
                    logging.info("No watched shows found in Trakt (fresh environment)")
            else:
                logging.info("No watched shows response from Trakt (fresh environment)")

            # Handle movies - check for None or empty
            if watched_movies:
                # Convert generator to list if needed (may already be done above in debug section)
                if not isinstance(watched_movies, list) and hasattr(watched_movies, "__iter__"):
                    watched_movies = list(watched_movies)

                if watched_movies:  # Check if list is not empty
                    for movie in watched_movies:
                        # Fix: Extract TMDB ID from keys list instead of non-existent ids.tmdb
                        tmdb_id = None
                        if hasattr(movie, "keys") and movie.keys:
                            for key_type, key_value in movie.keys:
                                if key_type == 'tmdb' and key_value:
                                    try:
                                        tmdb_id = int(key_value)
                                        break
                                    except (ValueError, TypeError):
                                        continue
                        
                        # Fallback: try legacy ids.tmdb format for compatibility
                        if tmdb_id is None and hasattr(movie, "ids") and hasattr(movie.ids, "tmdb") and movie.ids.tmdb:
                            try:
                                tmdb_id = int(movie.ids.tmdb)
                            except (ValueError, TypeError):
                                pass
                        
                        if tmdb_id:
                            self._watched_movies.add(tmdb_id)
                    
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

    def verifyAccountInfo(self):
        """Debug method to verify which account we're accessing and get basic stats"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                # Get user info
                user = Trakt["users/me"].get()
                if user:
                    logging.info("=== ACCOUNT VERIFICATION ===")
                    logging.info(f"Authenticated user: {user.username}")
                    # Fix: Proper access to user IDs - use slug if available, otherwise trakt ID
                    user_id = "unknown"
                    if hasattr(user, 'ids'):
                        if hasattr(user.ids, 'slug') and user.ids.slug:
                            user_id = user.ids.slug
                        elif hasattr(user.ids, 'trakt') and user.ids.trakt:
                            user_id = str(user.ids.trakt)
                    logging.info(f"User ID: {user_id}")
                    
                    # Get user stats (may fail for some accounts)
                    try:
                        stats = Trakt["users/me/stats"].get()
                        if stats:
                            logging.info(f"Profile stats - Episodes: {stats.episodes.watched}, Movies: {stats.movies.watched}")
                            logging.info(f"Shows: {stats.shows.watched}, Total plays: {stats.episodes.plays + stats.movies.plays}")
                        else:
                            logging.debug("Stats API returned None - this is normal for some account types")
                    except Exception as stats_error:
                        logging.debug(f"Stats API call failed (non-critical): {stats_error}")
                    
                    return user.username
                else:
                    logging.debug("Could not retrieve user information")
                    return None
        except Exception as e:
            logging.error(f"Error verifying account info: {e}")
            return None

    def getWatchedShows(self):
        """Retrieve all watched TV shows from Trakt with full episode data"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                # Use users/me/watched which returns full Show objects with episodes
                return Trakt["users/me/watched"].shows()
        except Exception as e:
            logging.error(f"Error getting watched shows: {e}")
            return None

    def getWatchedMovies(self):
        """Retrieve all watched movies from Trakt with full data"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                return Trakt["users/me/watched"].movies()
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
        
        # Enhanced debug logging for duplicate detection
        if not result and len(self._watched_episodes) > 0:
            logging.debug(f"Episode key '{episode_key}' not found in cache of {len(self._watched_episodes)} items")
            # Show a few cached keys for comparison
            sample_keys = [k for k in self._watched_episodes if isinstance(k, tuple) and len(k) == 3][:3]
            if sample_keys:
                logging.debug(f"Sample cached keys: {sample_keys}")
        
        logging.debug(
            f"isEpisodeWatched({show_name}, S{season_number:02d}E{episode_number:02d}) -> {result}"
        )
        return result

    def addMovie(self, movie_data: dict):
        """Add a movie to the pending sync buffer"""
        self._movies.append(movie_data)

    def addEpisodeToHistory(self, episode_data: dict, show_name: Optional[str] = None, season_number: Optional[int] = None, episode_number: Optional[int] = None):
        """Add an episode to the pending sync buffer and immediately cache it to prevent duplicates"""
        self._episodes.append(episode_data)
        
        # Immediately cache this episode to prevent re-import on subsequent runs
        # This fixes the bug where the same episodes are added repeatedly
        if show_name and season_number is not None and episode_number is not None:
            episode_key = (show_name.lower(), season_number, episode_number)
            self._watched_episodes.add(episode_key)
            logging.debug(f"Pre-cached episode for duplicate prevention: {show_name} S{season_number}E{episode_number}")

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
            self._user_message(f"Processing batch {batch_num}/{total_batches} ({len(batch)} episodes)", "info")

            # Enforce rate limit before each batch
            self._enforce_rate_limit(batch_delay)

            try:
                response = self._sync_batch_with_retry(
                    {"episodes": batch}, "episodes", batch_num, batch_delay
                )
                if response:
                    added = response.get("added", {}).get("episodes", 0)
                    added_total += added

                    # Reset consecutive failure counters on success
                    self._consecutive_rate_limits = 0
                    self._consecutive_auth_failures = 0
                    
                    # Update cache with successfully synced episodes
                    if added > 0:
                        self._update_episode_cache_after_sync(batch, added)
                    
                    # User feedback
                    self._user_message(f"Batch {batch_num} completed: {added}/{len(batch)} episodes added", "info")

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
                self._user_message(f"Batch {batch_num}: No response from Trakt API (possible token issue)", "warning")
                raise Exception("No response from Trakt API")

            return response

        except Exception as e:
            error_str = str(e).lower()
            
            # Check for authentication/token refresh issues
            if ("no response" in error_str or 
                "unable to refresh expired token" in error_str or
                "token refreshing hasn't been enabled" in error_str):
                self._consecutive_auth_failures += 1
                self._user_message(f"Authentication issue detected in batch {batch_num} (failure #{self._consecutive_auth_failures})", "warning")
                
                if self._consecutive_auth_failures >= self._max_auth_failures:
                    self._user_message(f"CRITICAL: {self._consecutive_auth_failures} consecutive authentication failures.", "critical")
                    self._user_message("Likely fix: Delete 'traktAuth.json' and re-run the script.", "critical")
                    self._user_message("Stopping sync to prevent further data loss...", "critical")
                    raise Exception(f"Authentication failed {self._consecutive_auth_failures} times consecutively. " +
                                   "Please delete 'traktAuth.json' and re-authenticate.")

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

    def _update_episode_cache_after_sync(self, episode_batch, successfully_added_count):
        """
        Update the episode cache with successfully synced episodes to prevent re-import.
        Note: Episodes should already be pre-cached when added to the sync queue.
        """
        logging.debug(f"Post-sync cache update: {successfully_added_count} episodes successfully synced")
        # Episodes are now pre-cached when added to sync queue via addEpisodeToHistory
        # No additional cache updates needed here

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
            self._user_message("Authentication has already been started", "warning")
            return False

        code_info = Trakt["oauth/device"].code()

        self._user_message(
            f'Enter the code "{code_info.get("user_code")}" at {code_info.get("verification_url")} to authenticate your Trakt account',
            "info"
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
        self._user_message("Authentication aborted", "warning")
        self._notify_auth_complete()

    def on_authenticated(self, authorization):
        """Called when user completes authentication successfully"""
        self.authorization = authorization
        self._user_message("Authentication successful!", "info")
        with open("traktAuth.json", "w") as f:
            json.dump(self.authorization, f)
        self._notify_auth_complete()

    def on_expired(self):
        """Called when auth times out or expires"""
        self._user_message("Authentication expired", "warning")
        self._notify_auth_complete()

    def on_poll(self, callback):
        """Called on every poll attempt during auth"""
        callback(True)

    def _notify_auth_complete(self):
        """Notify any threads waiting on authentication that it is complete"""
        self.is_authenticating.acquire()
        self.is_authenticating.notify_all()
        self.is_authenticating.release()
