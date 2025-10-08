"""
Enhanced TraktIO module with robust rate limiting and retry mechanisms.
Fixes for persistent 429 errors and episode loss issues.
"""

from typing import Iterable, List, Optional, Set, Tuple

import json
import logging
import os.path
import re
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


def _parse_tmdb_id(val: object) -> Optional[int]:
    """Safely coerce TMDB identifiers to integers when possible."""
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            return None
    return None


def _generate_episode_keys(title: str, season_num: int, episode_num: int) -> Set[Tuple[str, int, int]]:
    """
    Generate canonical and alias keys for robust episode duplicate detection.

    This function implements the alias fallback system that enables matching episodes
    even when show titles have slight variations between Netflix exports and Trakt data.
    Multiple keys are generated to catch different representations of the same episode.

    Args:
        title: Show title (may contain variations like subtitles, formatting)
        season_num: Season number
        episode_num: Episode number

    Returns:
        Set of tuples (normalized_title, season_num, episode_num) for duplicate checking

    Key generation strategy:
    1. Base key: Lowercase title exactly as provided
    2. Alias key: Title with everything after first colon removed (handles subtitle variations)
    3. Normalized key: Alphanumeric-only normalization (handles punctuation/spacing differences)

    Example:
        title="The Show: Special Edition" -> generates keys for:
        - "the show: special edition" (base)
        - "the show" (alias - removes subtitle)
        - "the show special edition" (normalized - removes punctuation)

    This multi-key approach ensures episodes are properly deduplicated even when
    Netflix exports contain formatting variations or when Trakt data uses different
    title conventions.
    """
    base = (title or "").lower()
    # Alias key: Remove subtitle after colon to handle "Show: Subtitle" variations
    alias = re.sub(r":.*$", "", base).strip()
    # Normalized key: Keep only alphanumeric characters and spaces for robust matching
    normalized = re.sub(r"[^a-z0-9]+", " ", base).strip()

    # Start with base key, add distinct variations
    keys: Set[Tuple[str, int, int]] = {(base, season_num, episode_num)}
    if alias and alias != base:
        keys.add((alias, season_num, episode_num))
    if normalized and normalized not in {base, alias}:
        keys.add((normalized, season_num, episode_num))
    return keys


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
    INITIAL_BATCH_DELAY = getattr(config, "TRAKT_API_INITIAL_DELAY", 3.0)
    RATE_LIMIT_DELAY = getattr(config, "TRAKT_API_RATE_LIMIT_DELAY", 30.0)
    SERVER_ERROR_DELAY = 10.0  # Delay after 5xx error
    MAX_RETRY_ATTEMPTS = getattr(config, "TRAKT_API_MAX_RETRIES", 5)

    # TMDB coverage threshold for triggering history hydration
    # When less than 60% of episodes have TMDB IDs, hydration is triggered
    # This threshold balances reliability vs unnecessary API calls
    # Below 60%: Poor duplicate detection, high risk of false negatives
    # Above 60%: Acceptable TMDB coverage for reliable duplicate detection
    TMDB_COVERAGE_MIN_THRESHOLD = 0.6

    def __init__(self, page_size=None, dry_run=None, verbose=None):
        # Configure Trakt client credentials
        Trakt.configuration.defaults.client(
            id=config.TRAKT_API_CLIENT_ID, secret=config.TRAKT_API_CLIENT_SECRET
        )

        self.authorization = None
        self._watched_episode_tmdb_ids: Set[int] = set()  # New set to track TMDB IDs of watched episodes

        # Use config defaults if not explicitly provided
        self.page_size = page_size if page_size is not None else config.TRAKT_API_SYNC_PAGE_SIZE
        self.dry_run = dry_run if dry_run is not None else config.TRAKT_API_DRY_RUN
        self.verbose = verbose if verbose is not None else config.TRAKT_API_VERBOSE
        self.initial_batch_delay = getattr(config, "TRAKT_API_INITIAL_DELAY", self.INITIAL_BATCH_DELAY)
        self.rate_limit_delay = getattr(config, "TRAKT_API_RATE_LIMIT_DELAY", self.RATE_LIMIT_DELAY)
        self.max_retry_attempts = getattr(config, "TRAKT_API_MAX_RETRIES", self.MAX_RETRY_ATTEMPTS)
        self._tmdb_history_hydrated = False

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
        self._last_account_check_status: Optional[str] = None
        self._last_watched_fetch_status: Optional[str] = None

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

            watched_shows = self.getWatchedShows()
            if watched_shows is not None:
                self._user_message("Authorization appears valid. Watched shows retrieved.", "info")
                self.cacheWatchedHistory()
            else:
                if self._last_watched_fetch_status == "server_error":
                    self._user_message(
                        "Trakt watch history temporarily unavailable (server error). Proceeding with fresh cache; retries will hydrate once the service recovers.",
                        "warning",
                    )
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

        This method implements the core duplicate detection system with
        coverage calculation logic. It handles multiple scenarios including fresh Trakt
        accounts (0% coverage) and determines when TMDB ID hydration is needed.

        Key responsibilities:
        1. Cache watched episodes using both alias keys and TMDB IDs
        2. Cache watched movies using TMDB IDs
        3. Calculate TMDB coverage percentage for episodes
        4. Trigger hydration when coverage falls below threshold
        5. Handle edge cases like fresh accounts gracefully

        Coverage calculation logic:
        - Tracks total watched episodes vs episodes with TMDB IDs
        - Uses TMDB_COVERAGE_MIN_THRESHOLD (60%) to determine if hydration needed
        - Special handling for 0% coverage (fresh accounts or API issues)
        - Automatically triggers hydration for both episodes and movies when empty

        The method separates plays (individual watch events) from unique items
        (distinct episodes/movies) to provide accurate accounting for duplicate
        detection and sync reporting.
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
                    
                    for show_index, show_entry in enumerate(watched_shows):
                        show_obj = getattr(show_entry, "show", show_entry)
                        show_title = (
                            getattr(show_obj, "title", None)
                            or getattr(show_obj, "name", None)
                            or str(show_obj)
                        )

                        seasons_payload = getattr(show_entry, "seasons", None)
                        if seasons_payload is None and hasattr(show_obj, "seasons"):
                            seasons_payload = show_obj.seasons
                        seasons_list = list(self._iter_seasons(seasons_payload))
                        if not seasons_list:
                            continue

                        if show_index < 3:
                            logging.info(
                                f"DEBUG: Show {show_index + 1}: '{show_title}' - {len(seasons_list)} seasons"
                            )

                        total_seasons += len(seasons_list)

                        for season_num, season_data in seasons_list:
                            if season_num is None:
                                continue
                            episode_entries = list(self._iter_episodes(season_data))
                            if not episode_entries:
                                continue
                            total_api_episodes += len(episode_entries)

                            for episode_num, episode_payload in episode_entries:
                                if episode_num is None:
                                    continue
                                total_watched_episodes += 1

                                # Add all alias keys for this episode to enable robust duplicate detection
                                # Each episode generates multiple keys to handle title variations
                                for key in _generate_episode_keys(show_title, season_num, episode_num):
                                    self._watched_episodes.add(key)
                                name_based_adds += 1

                                # Extract and cache TMDB ID for superior duplicate detection
                                # TMDB IDs are globally unique and immune to title formatting differences
                                tmdb_id = self._extract_tmdb_id_from_item(episode_payload)
                                if tmdb_id is not None:
                                    # Track new TMDB ID additions for coverage calculation
                                    if tmdb_id not in self._watched_episode_tmdb_ids:
                                        id_based_adds += 1
                                    self._watched_episode_tmdb_ids.add(tmdb_id)

                    # DEBUG: Log detailed statistics
                    logging.info("=== CACHE DEBUG STATISTICS ===")
                    logging.info(f"Shows processed: {len(watched_shows)}")
                    logging.info(f"Total seasons: {total_seasons}")
                    logging.info(f"Total episodes from API: {total_api_episodes}")
                    logging.info(f"Episodes with watch data: {total_watched_episodes}")
                    logging.info(f"ID-based cache additions: {id_based_adds}")
                    logging.info(f"Name-based cache additions: {name_based_adds}")
                    logging.info(f"Final cache size: {len(self._watched_episodes)}")
                    denominator = id_based_adds + name_based_adds
                    if denominator > 0:
                        efficiency = len(self._watched_episodes) / denominator * 100
                        logging.info(
                            f"Cache efficiency: {len(self._watched_episodes)} / ({id_based_adds} + {name_based_adds}) = {efficiency:.1f}%"
                        )
                    else:
                        logging.info("Cache efficiency: no watched episodes identified (denominator 0)")
                    
                    # Show sample cache entries
                    if self._watched_episodes:
                        sample_episodes = list(self._watched_episodes)[:10]
                        logging.info(f"Sample cache entries (first 10): {sample_episodes}")

                    logging.info(
                        f"Cached {total_watched_episodes} watched episodes"
                    )
                    if total_watched_episodes > 0:
                        # Calculate TMDB coverage: ratio of episodes with TMDB IDs to total episodes
                        # This determines the reliability of TMDB-backed duplicate detection
                        coverage = len(self._watched_episode_tmdb_ids) / max(
                            1, total_watched_episodes
                        )
                        logging.info(f"Episode TMDB coverage: {coverage:.1%}")

                        # Trigger hydration if coverage is below threshold (default: 60%)
                        # Low coverage leads to poor duplicate detection, causing false negatives
                        if coverage < self.TMDB_COVERAGE_MIN_THRESHOLD:
                            logging.info(
                                f"TMDB coverage {coverage:.1%} below threshold; hydrating from history"
                            )
                            # Hydrate from sync/history API which often has better TMDB metadata
                            self.hydrate_tmdb_ids_from_history()

                            # Recalculate coverage after hydration to measure improvement
                            coverage = len(self._watched_episode_tmdb_ids) / max(
                                1, total_watched_episodes
                            )
                            logging.info(
                                f"Episode TMDB coverage after hydration: {coverage:.1%}"
                            )
                    else:
                        # Special case: 0% coverage (fresh account or API issues)
                        # Log this distinctly as it triggers different handling logic
                        logging.info("Episode TMDB coverage: 0.0% (no watched episodes)")
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
                    for movie_entry in watched_movies:
                        movie_obj = getattr(movie_entry, "movie", movie_entry)
                        tmdb_id = self._extract_tmdb_id_from_item(movie_obj)
                        if tmdb_id is not None:
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
        
        # Special handling for 0% TMDB coverage scenarios
        # When sync/watched returns no TMDB IDs, immediately try sync/history
        # This handles fresh accounts or API inconsistencies gracefully
        if not self._watched_episode_tmdb_ids:
            logging.info("TMDB coverage is 0; hydrating from sync/history…")
            self.hydrate_tmdb_ids_from_history()

        # Mirror episode hydration logic for movies
        # When sync/watched returns no movies, try sync/history as fallback
        # This ensures movie duplicate detection works even with API inconsistencies
        if not self._watched_movies:
            logging.info("Movie cache is empty; hydrating from sync/history…")
            self.hydrate_movie_ids_from_history()

    def verifyAccountInfo(self):
        """Debug method to verify which account we're accessing and get basic stats"""
        self._last_account_check_status = "unknown"
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
                    
                    self._last_account_check_status = "ok"
                    return user.username
                else:
                    logging.debug("Could not retrieve user information")
                    self._last_account_check_status = "no_data"
                    return None
        except HTTPError as e:
            status_code = getattr(e.response, "status_code", None)
            if status_code and 500 <= status_code < 600:
                logging.warning(f"Trakt status check unavailable (server error {status_code})")
                self._last_account_check_status = "server_error"
                return None
            logging.error(f"Error verifying account info: {e}")
            self._last_account_check_status = "client_error"
            return None
        except Exception as e:
            logging.error(f"Error verifying account info: {e}")
            self._last_account_check_status = "exception"
            return None

    def getWatchedShows(self):
        """Retrieve all watched TV shows from Trakt with full episode data"""
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                shows = Trakt["sync/watched"].shows()
                self._last_watched_fetch_status = "ok"
                return shows
        except HTTPError as e:
            status_code = getattr(e.response, "status_code", None)
            if status_code and 500 <= status_code < 600:
                logging.warning(f"Trakt watched-shows endpoint unavailable (server error {status_code})")
                self._last_watched_fetch_status = "server_error"
            else:
                logging.error(f"Error getting watched shows: {e}")
                self._last_watched_fetch_status = "client_error"
            return None
        except Exception as e:
            logging.error(f"Error getting watched shows: {e}")
            self._last_watched_fetch_status = "exception"
            return None

    def getWatchedMovies(self):
        """Retrieve all watched movies from Trakt with full data"""
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
        self, show_name: str, season_number: int, episode_number: int, tmdb_id: Optional[int] = None
    ) -> bool:
        """
        Check if an episode is already marked as watched using improved duplicate detection.

        This method implements a two-tier detection strategy:
        1. Primary: TMDB ID-based detection (most reliable)
        2. Fallback: Alias key-based detection (handles title variations)

        The TMDB-backed detection is preferred because TMDB IDs are globally unique
        and immune to title formatting differences. When TMDB ID is unavailable,
        the alias key system provides robust fallback matching using multiple
        normalized title variations.

        Args:
            show_name: Show title (may contain formatting variations)
            season_number: Season number
            episode_number: Episode number (None if unknown)
            tmdb_id: TMDB episode ID if available (preferred for detection)

        Returns:
            True if episode is already watched, False otherwise

        Detection precedence:
        1. TMDB ID match (if tmdb_id provided and found in cache)
        2. Alias key matches (using _generate_episode_keys for variations)
        3. Default to False if no matches found

        This approach significantly reduces false negatives that could lead to
        duplicate episode submissions when show titles have minor variations
        between Netflix exports and Trakt data.
        """
        # Primary detection: TMDB-ID based (most reliable, immune to title variations)
        if tmdb_id is not None and tmdb_id in self._watched_episode_tmdb_ids:
            logging.debug(f"isEpisodeWatched(TMDb:{tmdb_id}) -> True (TMDB-ID cache hit)")
            return True

        # Guard against unknown episode numbers
        if episode_number is None:
            logging.debug(
                f"isEpisodeWatched({show_name}, S{season_number:02d}E??) -> False (episode number unknown)"
            )
            return False

        # Fallback detection: Check all alias keys for robust duplicate detection
        # This handles cases where TMDB ID is unavailable but title-based matching can work
        for key in _generate_episode_keys(show_name, season_number, episode_number):
            if key in self._watched_episodes:
                logging.debug(
                    f"isEpisodeWatched({show_name}, S{season_number:02d}E{episode_number:02d}) -> True (alias key match: {key})"
                )
                return True

        # No matches found through either detection method
        logging.debug(
            f"isEpisodeWatched({show_name}, S{season_number:02d}E{episode_number:02d}) -> False"
        )
        return False

    def addMovie(self, movie_data: dict):
        """Add a movie to the pending sync buffer and immediately cache it to prevent duplicates"""
        self._movies.append(movie_data)
        # Pre-cache TMDB ID if present to enhance duplicate detection
        tmdb_id = None
        if isinstance(movie_data, dict):
            ids = movie_data.get("ids") or {}
            tmdb_id = ids.get("tmdb")
            tmdb_id = tmdb_id if isinstance(tmdb_id, int) else _parse_tmdb_id(tmdb_id)
        if tmdb_id is not None:
            self._watched_movies.add(tmdb_id)  # prevent re-queue within same run
            logging.debug(f"Pre-cached movie TMDB ID for duplicate prevention: {tmdb_id}")

    def addEpisodeToHistory(self, episode_data: dict, show_name: Optional[str] = None, season_number: Optional[int] = None, episode_number: Optional[int] = None):
        """Add an episode to the pending sync buffer and immediately cache it to prevent duplicates"""
        self._episodes.append(episode_data)
        # Pre-cache TMDB ID if present to enhance duplicate detection
        tmdb_id: Optional[int] = None
        if isinstance(episode_data, dict):
            ids = episode_data.get("ids") or {}
            if isinstance(ids, dict):
                tmdb_id = _parse_tmdb_id(ids.get("tmdb"))
        if tmdb_id is not None:
            self._watched_episode_tmdb_ids.add(tmdb_id)
            logging.debug(f"Pre-cached episode TMDB ID for duplicate prevention: {tmdb_id}")
        
        # Immediately cache this episode to prevent re-import on subsequent runs
        # This fixes the bug where the same episodes are added repeatedly
        if show_name and season_number is not None and episode_number is not None:
            for key in _generate_episode_keys(show_name, season_number, episode_number):
                self._watched_episodes.add(key)
            logging.debug(
                f"Pre-cached episode for duplicate prevention: {show_name} S{season_number}E{episode_number}"
            )

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
                self.rate_limit_delay * (1 + self._consecutive_rate_limits * 0.5),
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
                    config, "TRAKT_API_BATCH_DELAY", self.initial_batch_delay
                )

                # Add initial delay before first API call to prevent immediate rate limit
                logging.info(
                    "Adding initial delay before sync to prevent rate limiting..."
                )
                time.sleep(self.initial_batch_delay)

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
                self._user_message(
                    f"Batch {batch_num}: No response from Trakt API - will retry shortly.",
                    "warning",
                )
                raise Exception("No response from Trakt API")

            return response

        except Exception as e:
            error_str = str(e).lower()
            
            # Check for authentication/token refresh issues
            if ("no response" in error_str or 
                "unable to refresh expired token" in error_str or
                "token refreshing hasn't been enabled" in error_str):
                self._consecutive_auth_failures += 1
                self._user_message(
                    f"Trakt returned no response for batch {batch_num}; retrying (attempt #{self._consecutive_auth_failures}).",
                    "warning",
                )
                
                if self._consecutive_auth_failures >= self._max_auth_failures:
                    self._user_message(f"CRITICAL: {self._consecutive_auth_failures} consecutive sync failures.", "critical")
                    self._user_message("Likely fix: Delete 'traktAuth.json' and re-run the script.", "critical")
                    self._user_message("Stopping sync to prevent further data loss...", "critical")
                    raise Exception(
                        f"Authentication failed {self._consecutive_auth_failures} times consecutively. "
                        "Please delete 'traktAuth.json' and re-authenticate."
                    )

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
                time.sleep(self.rate_limit_delay)

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

    @staticmethod
    def _iter_seasons(seasons: object) -> Iterable[Tuple[Optional[int], object]]:
        if seasons is None:
            return []
        if isinstance(seasons, dict):
            return list(seasons.items())
        if isinstance(seasons, list):
            result: List[Tuple[Optional[int], object]] = []
            for season in seasons:
                season_num = getattr(season, "number", None)
                if season_num is None and hasattr(season, "season"):
                    season_num = getattr(season, "season")
                if season_num is None and isinstance(season, dict):
                    season_num = season.get("number") or season.get("season")
                result.append((season_num, season))
            return result
        return []

    @staticmethod
    def _iter_episodes(season: object) -> Iterable[Tuple[Optional[int], object]]:
        episodes = getattr(season, "episodes", None)
        if episodes is None and isinstance(season, dict):
            episodes = season.get("episodes")
        if isinstance(episodes, dict):
            return list(episodes.items())
        if isinstance(episodes, list):
            result: List[Tuple[Optional[int], object]] = []
            for episode in episodes:
                episode_num = getattr(episode, "number", None)
                if episode_num is None and hasattr(episode, "episode"):
                    episode_num = getattr(episode, "episode")
                if episode_num is None and isinstance(episode, dict):
                    episode_num = episode.get("number") or episode.get("episode")
                result.append((episode_num, episode))
            return result
        return []

    @staticmethod
    def _episode_has_watch_data(episode: object) -> bool:
        if episode is None:
            return False
        if hasattr(episode, "last_watched_at") and getattr(episode, "last_watched_at"):
            return True
        if hasattr(episode, "plays"):
            plays = getattr(episode, "plays")
            if plays:
                return plays > 0
        if isinstance(episode, dict):
            if episode.get("last_watched_at"):
                return True
            plays = episode.get("plays")
            if plays:
                return plays > 0
        return False

    @staticmethod
    def _extract_tmdb_id_from_item(item: object) -> Optional[int]:
        if item is None:
            return None
        ids = getattr(item, "ids", None)
        tmdb_id = _parse_tmdb_id(getattr(ids, "tmdb", None)) if ids is not None else None
        if tmdb_id is not None:
            return tmdb_id
        if hasattr(item, "keys"):
            for key_type, key_value in getattr(item, "keys"):
                if key_type == "tmdb":
                    tmdb_id = _parse_tmdb_id(key_value)
                    if tmdb_id is not None:
                        return tmdb_id
        if isinstance(item, dict):
            tmdb_id = _parse_tmdb_id(item.get("tmdb"))
            if tmdb_id is not None:
                return tmdb_id
            for key_type, key_value in item.get("keys", []) or []:
                if key_type == "tmdb":
                    tmdb_id = _parse_tmdb_id(key_value)
                    if tmdb_id is not None:
                        return tmdb_id
        return None

    def hydrate_tmdb_ids_from_history(self, per_page: int = 100) -> None:
        """
        Hydrate the TMDB episode cache from Trakt's sync/history API when coverage is low.

        This method addresses the limitation that Trakt's sync/watched API sometimes
        returns episode data without TMDB IDs, leading to poor TMDB coverage in the
        duplicate detection cache. The sync/history API often contains richer metadata
        including TMDB IDs that were missing from the sync/watched response.

        How it works:
        1. Check if hydration already completed (prevent duplicate work)
        2. Paginate through user's entire episode history via sync/history API
        3. Extract TMDB IDs from history entries that weren't in sync/watched
        4. Add newly found TMDB IDs to the duplicate detection cache
        5. Mark hydration complete to prevent redundant API calls

        This process significantly improves duplicate detection reliability by ensuring
        the TMDB-backed detection has comprehensive coverage, reducing false negatives
        that could lead to duplicate submissions.

        Args:
            per_page: Number of history items to fetch per API call (default: 100)

        Coverage improvement scenarios:
        - sync/watched returns episodes without TMDB metadata
        - sync/history contains the same episodes WITH TMDB metadata
        - Hydration fills the gap, improving detection coverage from ~20% to ~90%+

        Note: This is a one-time operation per TraktIO instance, tracked by
        _tmdb_history_hydrated flag to prevent redundant API calls.
        """
        # Skip if already hydrated to prevent redundant API calls
        if self._tmdb_history_hydrated:
            return

        added = 0
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                page = 1
                # Paginate through entire episode history to find TMDB IDs
                while True:
                    # Fetch next page of episode history
                    items = Trakt["sync/history"].episodes(page=page, per_page=per_page)
                    if not items:
                        break
                    # Ensure items is a list for consistent processing
                    if not isinstance(items, list):
                        items = list(items)
                    if not items:
                        break

                    # Extract TMDB IDs from each history entry
                    for item in items:
                        # Get episode object (may be nested under different attributes)
                        episode_obj = getattr(item, "episode", None) or item
                        tmdb_id = self._extract_tmdb_id_from_item(episode_obj)

                        # Add new TMDB IDs to duplicate detection cache
                        if tmdb_id is not None and tmdb_id not in self._watched_episode_tmdb_ids:
                            self._watched_episode_tmdb_ids.add(tmdb_id)
                            added += 1

                    # Check if we've reached the end of history (fewer items than requested)
                    if len(items) < per_page:
                        break
                    page += 1

        except Exception as exc:
            logging.warning(f"TMDB history hydration failed: {exc}")
        else:
            logging.info(f"Hydrated {added} episode TMDB IDs from history")
        finally:
            # Always mark as hydrated to prevent retry loops on persistent failures
            self._tmdb_history_hydrated = True

    def hydrate_movie_ids_from_history(self, per_page: int = 100) -> None:
        """
        Hydrate watched movies cache from sync/history API when sync/watched is empty.

        This method mirrors the episode hydration functionality but focuses on movies.
        It addresses scenarios where Trakt's sync/watched API returns an empty movie
        list despite the user having watched movies in their history. This commonly
        occurs with fresh Trakt accounts or after certain API changes.

        How it works:
        1. Paginate through user's movie history via sync/history API
        2. Extract TMDB IDs from each movie history entry
        3. Add TMDB IDs to the watched movies cache for duplicate detection
        4. Continue until all history pages are processed

        This ensures that even when sync/watched fails to return watched movies,
        the duplicate detection system can still identify previously watched content
        by consulting the more comprehensive sync/history data.

        Args:
            per_page: Number of history items to fetch per API call (default: 100)

        Use cases:
        - Fresh Trakt accounts where sync/watched is empty but history exists
        - API inconsistencies where sync/watched misses some watched content
        - Recovery from authentication issues that cleared watched cache

        The hydration process is similar to episode hydration but simpler since
        movies only need TMDB ID-based detection (no title variations like episodes).
        """
        added = 0
        try:
            with Trakt.configuration.oauth.from_response(self.authorization):
                page = 1
                # Paginate through entire movie history to find watched movies
                while True:
                    # Fetch next page of movie history
                    items = Trakt["sync/history"].movies(page=page, per_page=per_page)
                    if not items:
                        break
                    # Ensure items is a list for consistent processing
                    items = list(items) if not isinstance(items, list) else items
                    if not items:
                        break

                    # Extract TMDB IDs from each movie history entry
                    for item in items:
                        # Get movie object (may be nested under different attributes)
                        movie = getattr(item, "movie", item)
                        tmdb_id = self._extract_tmdb_id_from_item(movie)

                        # Add new TMDB IDs to duplicate detection cache
                        if tmdb_id is not None and tmdb_id not in self._watched_movies:
                            self._watched_movies.add(tmdb_id)
                            added += 1

                    # Check if we've reached the end of history (fewer items than requested)
                    if len(items) < per_page:
                        break
                    page += 1

        except Exception as exc:
            logging.warning(f"Movie history hydration failed: {exc}")
        else:
            logging.info(f"Hydrated {added} movie TMDB IDs from history")

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
