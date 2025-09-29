#!/usr/bin/env python3
"""
Enhanced Netflix to Trakt Import Script
Adapted to work with existing NetflixTvShow module structure
Fixes rate limiting issues and improves error handling
"""

from __future__ import absolute_import, division, print_function

import csv
import os
import json
import logging
import re
from csv import writer as csv_writer

from tenacity import retry, stop_after_attempt, wait_random
from tmdbv3api import TV, Movie, Season, TMDb
from tmdbv3api.exceptions import TMDbException
from tqdm import tqdm
from trakt import Trakt

import config
from NetflixTvShow import NetflixTvHistory, NetflixMovie, NetflixTvShowEpisode
from TraktIO import TraktIO, _generate_episode_keys, _parse_tmdb_id

# Constants
EPISODES_AND_MOVIES_NOT_FOUND_FILE = "not_found.csv"

# Global tracking variables for comprehensive episode accounting
total_netflix_episodes = 0
total_processed_episodes = 0
total_episodes_added = 0
total_episodes_skipped_watched = 0
total_episodes_skipped_no_tmdb = 0
skipped_preexisting_snapshot = 0
skipped_within_run_duplicates = 0
snapshot_tmdb_coverage_percent = 0.0

# Snapshot and queue tracking (populated during runtime)
start_episode_snapshot = set()
start_episode_tmdb_snapshot = set()
start_movie_snapshot = set()

queued_unique_episode_markers = set()
queued_unique_movie_ids = set()
queued_movie_play_count = 0


def make_episode_unique_marker(show_name, season_number, episode_number, tmdb_id):
    """
    Generate a stable unique marker for episode accounting and deduplication.

    This function creates a consistent identifier for episodes that prioritizes
    TMDB IDs when available but falls back to title-based markers when necessary.
    The marker format enables separation of TMDB-backed vs uncertain episodes
    in accounting and reporting.

    Marker selection strategy:
    1. Primary: TMDB-based marker if tmdb_id is valid integer
    2. Fallback: Title-based marker using normalized show name

    Args:
        show_name: Show title (may contain formatting variations)
        season_number: Season number
        episode_number: Episode number
        tmdb_id: TMDB episode ID if available (preferred)

    Returns:
        Tuple in one of two formats:
        - ("tmdb", tmdb_id) for TMDB-backed episodes
        - ("slug", normalized_show, season_number, episode_number) for title-based

    The TMDB-backed format is preferred because:
    - TMDB IDs are globally unique and stable
    - Immune to title formatting variations
    - Provide reliable baseline for snapshot reconciliation

    The title-based fallback handles cases where TMDB matching failed,
    but still provides reasonable deduplication within the current run.
    """
    # Prefer TMDB-based marker for maximum reliability and global uniqueness
    if tmdb_id:
        try:
            return ("tmdb", int(tmdb_id))
        except (TypeError, ValueError):
            pass

    # Fallback to title-based marker when TMDB ID unavailable
    normalized_show = (show_name or "").lower()
    return ("slug", normalized_show, season_number, episode_number)


# TMDB API instances
tmdb_instance = TMDb()
tv_api = TV()
movie_api = Movie()
season_api = Season()


def append_not_found(show_or_movie, season=None, episode=None):
    """Append a not-found entry to the CSV (minimal, resilient)."""
    try:
        with open(EPISODES_AND_MOVIES_NOT_FOUND_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv_writer(f)
            w.writerow([
                show_or_movie or "UNKNOWN",
                season if season is not None else "",
                episode if episode is not None else "",
            ])
    except Exception as e:
        logging.debug(f"append_not_found failed: {e}")


class TMDBHelper:
    """Enhanced TMDB cache helper with better error handling"""
    
    def __init__(self, cache_file="tmdb_cache.json"):
        self.cache_file = cache_file
        self.cache = {}
        self.hits = 0
        self.misses = 0
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
            except json.JSONDecodeError:
                self.cache = {}

    def get_cached_result(self, title):
        """Get cached TMDB result for a title"""
        key = title.lower()
        if key in self.cache:
            self.hits += 1
            return self.cache[key]
        else:
            self.misses += 1
            return None

    def set_cached_result(self, title, result):
        """Cache a TMDB result"""
        key = title.lower()
        self.cache[key] = self._serialize_result(result)
        tmp_path = self.cache_file + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            # Atomic replace to avoid truncated files if interrupted mid-write
            os.replace(tmp_path, self.cache_file)
        except Exception as e:
            logging.debug(f"Failed writing TMDB cache for '{title}': {e}")
            self.cache.pop(key, None)
            # Cleanup temp if left behind
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception as cleanup_err:
                logging.debug(f"Temp cache cleanup failed: {cleanup_err}")

    def _serialize_result(self, result):
        """Reduce TMDB object to a JSON-serializable plain dict with recursive handling."""
        if result is None:
            return None
        
        # Handle different result types
        if hasattr(result, '__dict__'):
            # Convert object to dict recursively
            serialized = {}
            for key, value in result.__dict__.items():
                if key.startswith('_'):
                    continue
                serialized[key] = self._serialize_value(value)
            return serialized
        elif isinstance(result, dict):
            return self._serialize_dict(result)
        elif isinstance(result, list):
            return self._serialize_list(result)
        elif isinstance(result, (str, int, float, bool, type(None))):
            return result
        else:
            # For other objects, try to extract an ID or convert to string
            if hasattr(result, 'id'):
                return {"id": result.id}
            else:
                return str(result)

    def _serialize_value(self, value):
        """Recursively serialize a single value to ensure JSON compatibility."""
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, dict):
            return self._serialize_dict(value)
        elif isinstance(value, list):
            return self._serialize_list(value)
        elif hasattr(value, '__dict__'):
            # Recursively serialize objects with __dict__
            serialized = {}
            for key, nested_value in value.__dict__.items():
                if not key.startswith('_'):
                    serialized[key] = self._serialize_value(nested_value)
            return serialized
        else:
            # For other types, try to extract meaningful data or convert to string
            if hasattr(value, 'id'):
                return {"id": value.id}
            else:
                return str(value)

    def _serialize_dict(self, obj):
        """Recursively serialize dictionary contents."""
        serialized = {}
        for key, value in obj.items():
            serialized[key] = self._serialize_value(value)
        return serialized

    def _serialize_list(self, obj):
        """Recursively serialize list contents."""
        return [self._serialize_value(item) for item in obj]

    def log_summary(self):
        """Log cache efficiency summary"""
        total = self.hits + self.misses
        if total > 0:
            hit_rate = (self.hits / total) * 100
            logging.info(f"TMDB cache summary: hits={self.hits} misses={self.misses} "
                        f"hit_rate={hit_rate:.1f}% entries={len(self.cache)}")



def setupTMDB():
    """Initialize TMDB with configuration"""
    tmdb_instance.api_key = config.TMDB_API_KEY
    tmdb_instance.language = config.TMDB_LANGUAGE
    tmdb_instance.debug = config.TMDB_DEBUG
    return tmdb_instance


# ========== ENHANCED TMDB SEARCH HELPERS ==========

def normalize_show_title(title):
    """
    Normalize show title for better TMDB matching.
    Removes common patterns that cause lookup failures.
    """
    if not title:
        return title
        
    # Create normalized version
    normalized = title.strip()
    
    # Remove common Netflix formatting artifacts
    normalized = re.sub(r'\s*:\s*Season\s+\d+.*$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s*:\s*Series\s+\d+.*$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s*\(.*\)\s*$', '', normalized)  # Remove trailing parentheses
    
    # Clean up extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def get_title_variations(original_title):
    """
    Generate title variations for enhanced TMDB searching.
    Returns list of alternative titles to try.
    """
    variations = []
    
    # Add original title
    variations.append(original_title)
    
    # Add normalized version
    normalized = normalize_show_title(original_title)
    if normalized != original_title:
        variations.append(normalized)
    
    # Try without subtitle after colon
    if ':' in original_title:
        main_title = original_title.split(':')[0].strip()
        if main_title not in variations:
            variations.append(main_title)
            
        # Try with "The" prefix if missing
        if not main_title.lower().startswith('the '):
            variations.append(f"The {main_title}")
    
    # Try adding common documentary/series patterns for short titles
    if len(original_title) < 20 and not any(word in original_title.lower() for word in [':', 'documentary', 'series']):
        doc_variations = [
            f"{original_title}: Documentary",
            f"{original_title} Documentary", 
            f"{original_title} Series",
            f"{original_title}: Limited Series"
        ]
        variations.extend(doc_variations)
    
    # Try removing "The" prefix if present
    if original_title.lower().startswith('the '):
        without_the = original_title[4:]
        if without_the not in variations:
            variations.append(without_the)
    
    # Handle common Netflix formatting quirks
    if ' - ' in original_title:
        # Try converting " - " to ": "
        colon_version = original_title.replace(' - ', ': ')
        if colon_version not in variations:
            variations.append(colon_version)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for variation in variations:
        if variation not in seen:
            seen.add(variation)
            unique_variations.append(variation)
    
    return unique_variations


def get_known_title_mappings():
    """
    Return dictionary of known problematic Netflix->TMDB title mappings.
    These are common cases where Netflix format exports differ from TMDB titles.
    """
    return {
        # Documentary series with missing sub-titles
        "World War II": "World War II in Colour",
        "American Murder": "American Murder: The Family Next Door", 
        "Into the Fire": "Into the Fire: The Lost Daughter",
        "Night Stalker": "Night Stalker: The Hunt for a Serial Killer",
        "Sins of Our Mother": "Sins of Our Mother",
        
        # Limited series and HBO content
        "The Pacific": "The Pacific",  # HBO series
        "Baby Reindeer": "Baby Reindeer",
        "Bodies": "Bodies",
        "Unbelievable": "Unbelievable", 
        "One Day": "One Day",
        "The Playlist": "The Playlist",
        "Griselda": "Griselda",
        "Eric": "Eric",
        
        # True crime and documentaries
        "Gone Girls": "Gone Girl",  # Alternative title
        "The Man with 1000 Kids": "The Man with 1000 Kids",
        "Dancing for the Devil": "Dancing for the Devil: The 7M TikTok Cult",
        "A Nearly Normal Family": "A Nearly Normal Family",
        "Dear Child": "Dear Child",
        "American Nightmare": "American Nightmare",
        
        # Common alternative titles
        "La Palma": "La Palma",
        "The Madness": "The Madness",
        "Adolescence": "Adolescence",
        
        # Episode-specific titles that need mapping to main series
        "Fake Profile: Killer Match: The Forbidden Dream": "Fake Profile",
        "How to Change Your Mind: Limited Series": "How to Change Your Mind",
        "Cheer: Season 2": "Cheer",
        "The Innocence Files: Limited Series": "The Innocence Files",
        "Car Masters: Rust to Riches": "Car Masters: Rust to Riches",  # Already correct
        
        # Add more mappings as patterns are discovered
    }


def enhanced_show_search(show_name, tv_api):
    """
    Enhanced TMDB show search with multiple fallback strategies.
    Returns (tmdb_id, search_method) or (None, None) if not found.
    """
    search_attempts = []
    
    # Strategy 1: Try exact search (current behavior)
    try:
        results = tv_api.search(show_name)
        if results:
            tmdb_id = results[0].id if hasattr(results[0], 'id') else results[0].get('id')
            return tmdb_id, "exact_match"
    except Exception as e:
        search_attempts.append(f"exact_search_failed: {e}")
    
    # Strategy 2: Try known mappings
    mappings = get_known_title_mappings()
    if show_name in mappings:
        mapped_title = mappings[show_name]
        try:
            results = tv_api.search(mapped_title)
            if results:
                tmdb_id = results[0].id if hasattr(results[0], 'id') else results[0].get('id')
                logging.info(f"TMDB Enhanced: Found '{show_name}' using mapping -> '{mapped_title}' (ID: {tmdb_id})")
                return tmdb_id, "known_mapping"
        except Exception as e:
            search_attempts.append(f"mapped_search_failed: {e}")
    
    # Strategy 3: Try title variations
    variations = get_title_variations(show_name)
    for i, variation in enumerate(variations[1:], 1):  # Skip original (already tried)
        try:
            results = tv_api.search(variation)
            if results:
                tmdb_id = results[0].id if hasattr(results[0], 'id') else results[0].get('id')
                logging.info(f"TMDB Enhanced: Found '{show_name}' using variation -> '{variation}' (ID: {tmdb_id})")
                return tmdb_id, f"variation_{i}"
        except Exception as e:
            search_attempts.append(f"variation_{i}_failed: {e}")
    
    # All strategies failed
    if search_attempts:
        logging.debug(f"TMDB Enhanced: All search strategies failed for '{show_name}': {search_attempts}")
    
    return None, None


@retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=3))
def getShowInformationFromTMDB(show_name, tmdb_cache):
    """
    Fetch show information from TMDB API with caching and enhanced search capabilities.
    Returns tmdb_id or None if not found.
    """
    # Check cache first
    cached = tmdb_cache.get_cached_result(f"show_{show_name}")
    if cached is not None:
        logging.debug(f"TMDB cache hit for show: {show_name}")
        return cached.get("id") if isinstance(cached, dict) else cached
    
    # Cache miss - fetch from API with enhanced search
    logging.debug(f"TMDB cache miss for show: {show_name}, fetching from API")
    
    try:
        # Try enhanced search with multiple fallback strategies
        tmdb_id, search_method = enhanced_show_search(show_name, tv_api)
        
        if tmdb_id:
            # Success - cache and return
            tmdb_cache.set_cached_result(f"show_{show_name}", {"id": tmdb_id})
            if search_method == "exact_match":
                logging.debug(f"TMDB API found show: {show_name} -> {tmdb_id}")
            else:
                logging.info(f"TMDB Enhanced search found show: {show_name} -> {tmdb_id} (method: {search_method})")
            return tmdb_id
        else:
            # All search strategies failed
            tmdb_cache.set_cached_result(f"show_{show_name}", None)
            logging.debug(f"TMDB Enhanced search could not find show: {show_name}")
            return None
            
    except TMDbException as e:
        logging.debug(f"TMDB API specific error for show {show_name}: {e}")
        tmdb_cache.set_cached_result(f"show_{show_name}", None)
        return None
    except Exception as e:
        logging.debug(f"TMDB API error for show {show_name}: {e}")
        tmdb_cache.set_cached_result(f"show_{show_name}", None)
        return None


@retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=3))
def getSeasonInformationFromTMDB(show_tmdb_id, season_number, tmdb_cache):
    """
    Fetch season information from TMDB API with caching.
    Returns season data or None if not found.
    """
    cache_key = f"season_{show_tmdb_id}_{season_number}"
    
    # Check cache first
    cached = tmdb_cache.get_cached_result(cache_key)
    if cached is not None:
        logging.debug(f"TMDB cache hit for season: show={show_tmdb_id}, season={season_number}")
        return cached
    
    # Cache miss - fetch from API
    logging.debug(f"TMDB cache miss for season: show={show_tmdb_id}, season={season_number}")
    
    try:
        season_data = season_api.details(show_tmdb_id, season_number)
        
        # Convert to serializable format
        if season_data:
            season_dict = {
                "id": season_data.id if hasattr(season_data, 'id') else None,
                "season_number": season_number,
                "episodes": []
            }
            
            if hasattr(season_data, 'episodes'):
                for ep in season_data.episodes:
                    episode_dict = {
                        "id": ep.id if hasattr(ep, 'id') else None,
                        "name": ep.name if hasattr(ep, 'name') else None,
                        "episode_number": ep.episode_number if hasattr(ep, 'episode_number') else None
                    }
                    season_dict["episodes"].append(episode_dict)
            
            tmdb_cache.set_cached_result(cache_key, season_dict)
            return season_dict
        else:
            tmdb_cache.set_cached_result(cache_key, None)
            return None
            
    except TMDbException as e:
        logging.debug(f"TMDB API specific error for season: {e}")
        tmdb_cache.set_cached_result(cache_key, None)
        return None
    except Exception as e:
        logging.debug(f"TMDB API error for season: {e}")
        tmdb_cache.set_cached_result(cache_key, None)
        return None


def enhanced_movie_search(movie_name, movie_api):
    """
    Enhanced TMDB movie search with multiple fallback strategies.
    Returns (tmdb_id, search_method) or (None, None) if not found.
    """
    search_attempts = []
    
    # Strategy 1: Try exact search (current behavior)
    try:
        results = movie_api.search(movie_name)
        if results:
            tmdb_id = results[0].id if hasattr(results[0], 'id') else results[0].get('id')
            return tmdb_id, "exact_match"
    except Exception as e:
        search_attempts.append(f"exact_search_failed: {e}")
    
    # Strategy 2: Try title variations
    variations = get_title_variations(movie_name)
    for i, variation in enumerate(variations[1:], 1):  # Skip original (already tried)
        try:
            results = movie_api.search(variation)
            if results:
                tmdb_id = results[0].id if hasattr(results[0], 'id') else results[0].get('id')
                logging.info(f"TMDB Enhanced: Found movie '{movie_name}' using variation -> '{variation}' (ID: {tmdb_id})")
                return tmdb_id, f"variation_{i}"
        except Exception as e:
            search_attempts.append(f"variation_{i}_failed: {e}")
    
    # All strategies failed
    if search_attempts:
        logging.debug(f"TMDB Enhanced: All movie search strategies failed for '{movie_name}': {search_attempts}")
    
    return None, None


@retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=3))
def getMovieInformationFromTMDB(movie_name, tmdb_cache):
    """
    Fetch movie information from TMDB API with caching and enhanced search capabilities.
    Returns tmdb_id or None if not found.
    """
    # Check cache first
    cached = tmdb_cache.get_cached_result(f"movie_{movie_name}")
    if cached is not None:
        logging.debug(f"TMDB cache hit for movie: {movie_name}")
        return cached.get("id") if isinstance(cached, dict) else cached
    
    # Cache miss - fetch from API with enhanced search
    logging.debug(f"TMDB cache miss for movie: {movie_name}, fetching from API")
    
    try:
        # Try enhanced search with multiple fallback strategies
        tmdb_id, search_method = enhanced_movie_search(movie_name, movie_api)
        
        if tmdb_id:
            # Success - cache and return
            tmdb_cache.set_cached_result(f"movie_{movie_name}", {"id": tmdb_id})
            if search_method == "exact_match":
                logging.debug(f"TMDB API found movie: {movie_name} -> {tmdb_id}")
            else:
                logging.info(f"TMDB Enhanced search found movie: {movie_name} -> {tmdb_id} (method: {search_method})")
            return tmdb_id
        else:
            # All search strategies failed
            tmdb_cache.set_cached_result(f"movie_{movie_name}", None)
            logging.debug(f"TMDB Enhanced search could not find movie: {movie_name}")
            return None
            
    except TMDbException as e:
        logging.debug(f"TMDB API specific error for movie {movie_name}: {e}")
        tmdb_cache.set_cached_result(f"movie_{movie_name}", None)
        return None
    except Exception as e:
        logging.debug(f"TMDB API error for movie {movie_name}: {e}")
        tmdb_cache.set_cached_result(f"movie_{movie_name}", None)
        return None

def processShow(show, traktIO, tmdb_cache):
    """Process a TV show and add all its episodes to Trakt"""
    global total_netflix_episodes, total_processed_episodes
    global total_episodes_added, total_episodes_skipped_watched, total_episodes_skipped_no_tmdb
    global skipped_preexisting_snapshot, skipped_within_run_duplicates
    global queued_unique_episode_markers
    
    # Count total episodes in this show
    episode_count = sum(len(season.episodes) for season in show.seasons)
    total_netflix_episodes += episode_count
    
    # Get show TMDB ID
    show_tmdb_id = getShowInformationFromTMDB(show.name, tmdb_cache)
    
    if show_tmdb_id is None:
        logging.warning(f"Show not found on TMDB: {show.name}")
        total_episodes_skipped_no_tmdb += episode_count
        total_processed_episodes += episode_count
        for season in show.seasons:
            for episode in season.episodes:
                append_not_found(show.name, season.number, episode.name)
        return
    
    # Process each season
    for season in show.seasons:
        season_data = getSeasonInformationFromTMDB(show_tmdb_id, season.number, tmdb_cache)
        
        if season_data is None:
            season_info = f"S{season.number}"
            if season.name:
                season_info += f" ({season.name})"
            logging.warning(f"Season not found on TMDB: {show.name} {season_info}")
            episode_count = len(season.episodes)
            total_episodes_skipped_no_tmdb += episode_count
            total_processed_episodes += episode_count
            for episode in season.episodes:
                append_not_found(show.name, season.number, episode.name)
            continue
        
        # Process each episode in the season
        for episode in season.episodes:
            total_processed_episodes += 1
            
            # Try to match episode
            matched = False
            episode_number = None
            episode_tmdb_id = None
            
            # First try: exact name match
            if season_data and "episodes" in season_data:
                for tmdb_episode in season_data["episodes"]:
                    if tmdb_episode.get("name") == episode.name:
                        episode_number = tmdb_episode.get("episode_number")
                        episode_tmdb_id = tmdb_episode.get("id")
                        matched = True
                        break
            
            # Second try: episode number in title (including roman numerals)
            if not matched:
                # Try regular episode numbers first
                match = re.search(r"(?:Episode|Ep\.?)\s*(\d+)", episode.name, re.IGNORECASE)
                if match:
                    episode_number = int(match.group(1))
                    if season_data and "episodes" in season_data:
                        for tmdb_episode in season_data["episodes"]:
                            if tmdb_episode.get("episode_number") == episode_number:
                                episode_tmdb_id = tmdb_episode.get("id")
                                matched = True
                                break
                
                # Try roman numerals (I, II, III, IV, V, etc.) and Part patterns
                if not matched:
                    # Extended roman numeral patterns including Part/Episode prefixes
                    roman_patterns = [
                        r"\b([IVX]{1,4})\b",  # Standalone roman numerals
                        r"Part\s+([IVX]{1,4})\b",  # "Part III"
                        r"Episode\s+([IVX]{1,4})\b",  # "Episode IV"
                    ]
                    
                    for pattern in roman_patterns:
                        roman_match = re.search(pattern, episode.name, re.IGNORECASE)
                        if roman_match:
                            roman_numeral = roman_match.group(1)
                            # Convert roman to decimal (extended to XXV)
                            roman_to_decimal = {
                                'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
                                'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
                                'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15,
                                'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19, 'XX': 20,
                                'XXI': 21, 'XXII': 22, 'XXIII': 23, 'XXIV': 24, 'XXV': 25
                            }
                            if roman_numeral in roman_to_decimal:
                                episode_number = roman_to_decimal[roman_numeral]
                                logging.debug(f"Found roman numeral episode: {roman_numeral} -> {episode_number} (pattern: {pattern})")
                                if season_data and "episodes" in season_data:
                                    for tmdb_episode in season_data["episodes"]:
                                        if tmdb_episode.get("episode_number") == episode_number:
                                            episode_tmdb_id = tmdb_episode.get("id")
                                            matched = True
                                            break
                                if matched:
                                    break
                        if matched:
                            break
            
            # Third try: estimate based on viewing order
            if not matched and season_data and "episodes" in season_data:
                total_episodes_in_season = len(season_data["episodes"])
                watched_episodes_in_season = len(season.episodes)
                if total_episodes_in_season == watched_episodes_in_season:
                    # Assume watched in order
                    episode_index = season.episodes.index(episode)
                    if episode_index < total_episodes_in_season:
                        episode_number = episode_index + 1
                        episode_tmdb_id = season_data["episodes"][episode_index].get("id")
                        matched = True
            
            # Add to Trakt if matched
            if matched and episode_tmdb_id:
                # Set the TMDB ID on the episode object
                if isinstance(episode, NetflixTvShowEpisode):
                    episode.setTmdbId(episode_tmdb_id)
                
                # Normalize TMDB ID to integer for consistent duplicate detection
                tmdb_numeric_id = _parse_tmdb_id(episode_tmdb_id)
                if tmdb_numeric_id is not None:
                    episode_tmdb_id = tmdb_numeric_id

                # Generate multiple alias keys for robust duplicate detection
                # This handles title variations between Netflix exports and Trakt data
                episode_keys = _generate_episode_keys(show.name, season.number, episode_number)

                # Dual-tier duplicate detection: TMDB-backed (preferred) + alias key fallback
                # TMDB detection: Check if episode ID exists in baseline snapshot
                preexisting_by_tmdb = (
                    tmdb_numeric_id is not None and tmdb_numeric_id in start_episode_tmdb_snapshot
                )
                # Alias key detection: Check if any generated key exists in baseline snapshot
                preexisting_by_key = any(key in start_episode_snapshot for key in episode_keys)

                # Master duplicate check using improved isEpisodeWatched logic
                # This method implements the two-tier detection internally
                if traktIO.isEpisodeWatched(show.name, season.number, episode_number, tmdb_numeric_id):
                    logging.info(f"Episode already watched: {show.name} S{season.number}E{episode_number}")
                    total_episodes_skipped_watched += 1

                    # Classify duplicate source for accurate accounting
                    # Preexisting: Found in baseline snapshot (existing Trakt data)
                    # Within-run: Duplicate within current CSV processing session
                    if preexisting_by_tmdb or preexisting_by_key:
                        skipped_preexisting_snapshot += 1
                    else:
                        skipped_within_run_duplicates += 1
                else:
                    # Track unique episodes vs individual plays for accurate accounting
                    # Only count as new unique episode if not in baseline snapshots
                    if not preexisting_by_tmdb and not preexisting_by_key:
                        marker = make_episode_unique_marker(
                            show.name,
                            season.number,
                            episode_number,
                            tmdb_numeric_id if tmdb_numeric_id is not None else episode_tmdb_id,
                        )
                        # Debug log to catch TMDb ID type mismatches
                        if marker[0] == "tmdb" and marker[1] not in start_episode_tmdb_snapshot:
                            logging.debug(f"TMDb marker not in baseline: {marker[1]} (type: {type(marker[1])})")
                        # Track unique episode marker (separates from play count)
                        queued_unique_episode_markers.add(marker)

                    # Add individual episode plays to Trakt queue
                    # Key distinction: This counts plays (watch events), not unique episodes
                    # A single episode may have multiple watch events (rewatches)
                    for watched_at in episode.watchedAt:
                        episode_data = {
                            "watched_at": watched_at,
                            "ids": {"tmdb": episode_tmdb_id}
                        }
                        # Pass show/season/episode info for immediate caching to prevent duplicates
                        traktIO.addEpisodeToHistory(episode_data, show.name, season.number, episode_number)
                        logging.info(f"Adding episode: {show.name} S{season.number}E{episode_number}")
                    # total_episodes_added tracks plays, not unique episodes
                    total_episodes_added += len(episode.watchedAt)
            else:
                logging.warning(f"Episode not matched: {show.name} S{season.number} - {episode.name}")
                total_episodes_skipped_no_tmdb += 1
                append_not_found(show.name, season.number, episode.name)


def processMovie(movie: NetflixMovie, traktIO, tmdb_cache):
    """Process a movie and add to Trakt if found on TMDB"""
    global queued_movie_play_count, queued_unique_movie_ids
    # Get movie TMDB ID
    tmdb_id = getMovieInformationFromTMDB(movie.name, tmdb_cache)
    
    if tmdb_id:
        movie.tmdbId = tmdb_id
        
        # Check if already watched
        if traktIO.isMovieWatched(tmdb_id):
            logging.info(f"Movie already watched: {movie.name}")
            return "skipped"
        
        # Deduplicate watch times for this movie while preserving play count
        unique_watch_times = set(movie.watchedAt)
        # Track total plays across all movies (includes rewatches)
        queued_movie_play_count += len(unique_watch_times)

        # Track unique movies separately from plays for accurate accounting
        # Only count as new unique movie if not in baseline snapshot
        if tmdb_id not in start_movie_snapshot:
            queued_unique_movie_ids.add(tmdb_id)

        # Add individual movie plays to Trakt queue
        # Key distinction: Each play is a separate watch event, even for same movie
        for watched_time in unique_watch_times:
            logging.info(f"Adding movie to trakt: {movie.name}")
            movie_data = {
                "title": movie.name,
                "watched_at": watched_time,
                "ids": {"tmdb": tmdb_id},
            }
            traktIO.addMovie(movie_data)
        return "added"
    else:
        logging.warning(f"Movie not found on TMDB: {movie.name}")
        append_not_found(movie.name)
        return "not_found"


def syncToTrakt(
    traktIO,
    expected_episode_plays=None,
    unique_episode_count=None,
    movie_play_count=None,
    tmdb_marker_count=0,
    slug_marker_count=0,
    snapshot_tmdb_coverage=None,
):
    """
    Final sync call to Trakt API with comprehensive logging and error handling.
    Reports only server-side outcomes (added, rejected as duplicate, API failures).
    Local pre-queue skips are computed and printed in main().
    Safe: getData() is a local buffer; len() here does not hit the network.
    """
    # Snapshot the queue once and reuse
    data_to_sync = traktIO.getData()
    episode_count = len(data_to_sync.get("episodes", []))
    movie_count   = len(data_to_sync.get("movies", []))

    # Defaults: expected plays == what’s in the queue; unique == tmdb+slug markers
    if expected_episode_plays is None:
        expected_episode_plays = episode_count
    if unique_episode_count is None:
        unique_episode_count = int(tmdb_marker_count or 0) + int(slug_marker_count or 0)
        

    # Logging + sanity check
    logging.info("=== FINAL SYNC TO TRAKT ===")
    logging.info(f"Movies queued for sync: {movie_count}")
    logging.info(f"Episodes queued for sync: {episode_count}")
    logging.info(f"Total items queued for sync: {movie_count + episode_count}")

    if snapshot_tmdb_coverage is not None:
        logging.info(f"Start snapshot TMDb coverage: {snapshot_tmdb_coverage:.1f}%")

    if episode_count != expected_episode_plays:
        logging.error(
            f"SYNC QUEUE MISMATCH: Expected {expected_episode_plays} episodes in sync queue, found {episode_count}"
        )

    # Pretty preamble
    batch_size = getattr(traktIO, "page_size", 30)
    ep_batches = (episode_count + batch_size - 1) // batch_size if episode_count else 0
    mv_batches = (movie_count   + batch_size - 1) // batch_size if movie_count   else 0
    total_batches = ep_batches + mv_batches

    resolved_movie_play_count = movie_play_count if movie_play_count is not None else movie_count

    parts = []
    if movie_count:
        parts.append(f"{resolved_movie_play_count} movie plays")
    if episode_count:
        ep_phrase = f"{episode_count} episode plays"
        if unique_episode_count:
            ep_phrase += f" (across {unique_episode_count} unique episodes)"
        parts.append(ep_phrase)

    if parts:
        print(f"\nSubmitting {' and '.join(parts)} to Trakt...")
        est_min = max(1, int(total_batches * 0.5))  # ~30s per batch
        print(f"Note: This may take about {est_min if est_min > 1 else 1} minute{'s' if est_min != 1 else ''} due to rate limiting protection.")
    else:
        print("\nNo items to submit to Trakt.")

    # Time the sync
    import time
    t0 = time.time()
    response = traktIO.sync()
    dt = time.time() - t0

    # Summarize results
    minutes, seconds = int(dt // 60), int(dt % 60)
    duration_str = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"

    if response:
        added   = response.get("added",   {})
        failed  = response.get("failed",  {})
        raw_m   = added.get("movies",   0)
        raw_e   = added.get("episodes", 0)

        added_movies    = len(raw_m) if isinstance(raw_m, list) else int(raw_m or 0)
        added_episodes  = len(raw_e) if isinstance(raw_e, list) else int(raw_e or 0)
        failed_movies   = int(failed.get("movies",   0) or 0)
        failed_episodes = int(failed.get("episodes", 0) or 0)
        
        # Pre-submission duplicate detection counters exist elsewhere:
        #   - Episodes: "skipped locally (already in start snapshot)" and
        #               "skipped locally (duplicate within this CSV run)".
        #   - Movies:   analogous local skip counters in main().
        # In this function we report server-side results only:
        #   duplicates rejected by Trakt during the sync (API-grounded),
        #   and API failures. Local skips are printed earlier.

        skipped_movies_by_trakt   = max(0, movie_count   - added_movies   - failed_movies)
        skipped_episodes_by_trakt = max(0, episode_count - added_episodes - failed_episodes)

        print(f"\n Trakt sync complete! (completed in {duration_str})")
        print(f"Added: {added_movies} movies, {added_episodes} episodes")
        if skipped_movies_by_trakt or skipped_episodes_by_trakt:
            print(f"Skipped by Trakt (duplicates): {skipped_movies_by_trakt} movies, {skipped_episodes_by_trakt} episodes")
        if failed_movies or failed_episodes:
            print(f"FAILED (API errors): {failed_movies} movies, {failed_episodes} episodes")
            print("Check the log file for details on failed items.")
    else:
        print("\n Sync failed — see log for details. Counts above reflect queued items only.")

    return response


def main():
    """Entry point: loads config, parses history, syncs Trakt"""
    global total_netflix_episodes, total_processed_episodes
    global total_episodes_added, total_episodes_skipped_watched, total_episodes_skipped_no_tmdb
    global skipped_preexisting_snapshot, skipped_within_run_duplicates
    global start_episode_snapshot, start_episode_tmdb_snapshot, start_movie_snapshot
    global queued_unique_episode_markers, queued_unique_movie_ids, queued_movie_play_count
    global snapshot_tmdb_coverage_percent
    
    # Reset global counters for clean re-runs within same interpreter
    total_netflix_episodes = 0
    total_processed_episodes = 0
    total_episodes_added = 0
    total_episodes_skipped_watched = 0
    total_episodes_skipped_no_tmdb = 0
    skipped_preexisting_snapshot = 0
    skipped_within_run_duplicates = 0
    
    # Initialize not_found.csv file
    with open(EPISODES_AND_MOVIES_NOT_FOUND_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv_writer(f)
        writer.writerow(["Show", "Season", "Episode"])
    
    # Set up logging
    logging.basicConfig(
        filename=config.LOG_FILENAME,
        level=config.LOG_LEVEL,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Configure TMDB
    setupTMDB()
    tmdb_cache = TMDBHelper()
    
    # Configure Trakt (uses config.ini settings automatically)
    traktIO = TraktIO()
    
    # Snapshot starting counts (don't let them grow mid-run)
    start_episode_snapshot = set(traktIO._watched_episodes)

    def _norm_title(t: str) -> str:
        """
        Normalize title strings for deduplicating alias keys in episode accounting.

        This function implements the key normalization strategy used to collapse
        multiple alias keys back to a single canonical representation for
        accurate unique episode counting.

        Normalization process:
        1. Convert to lowercase for case-insensitive comparison
        2. Remove subtitle text after first colon (handles "Show: Season X" patterns)
        3. Replace all non-alphanumeric characters with spaces
        4. Strip leading/trailing whitespace

        Args:
            t: Title string to normalize (may be None)

        Returns:
            Normalized title string suitable for deduplication

        Example transformations:
            "The Show: Special Edition" -> "the show"
            "Action-Adventure: Part II" -> "action adventure"
            "Documentary (2023)" -> "documentary 2023"

        This normalization ensures that different alias keys generated by
        _generate_episode_keys() for the same show collapse to a single
        canonical form when counting unique episodes, preventing double-counting
        in statistical reporting.
        """
        import re
        t = (t or "").lower()
        t = re.sub(r":.*$", "", t)                 # drop text after colon
        t = re.sub(r"[^a-z0-9]+", " ", t).strip()  # alnum normalize
        return t

    # collapse alias/base/normalized keys back to a single canonical key per episode
    start_episode_unique = {
        (_norm_title(k[0]), k[1], k[2])
        for k in start_episode_snapshot
        if isinstance(k, tuple) and len(k) == 3
    }

    # prefer a true number if stats API succeeded, otherwise fall back to our deduped estimate
    initial_watched_eps = (
        len(start_episode_unique)
    )

    start_episode_tmdb_snapshot = set(traktIO._watched_episode_tmdb_ids)

    start_movie_snapshot = set(traktIO._watched_movies)
    queued_unique_episode_markers = set()
    queued_unique_movie_ids = set()
    queued_movie_play_count = 0

    initial_watched_movies = len(start_movie_snapshot)

    print(f"Starting unique episodes in Trakt (snapshot): {initial_watched_eps:,}")
    print(f"(Internal cache keys for dup-detection: {len(start_episode_snapshot):,})")
    # Snapshot reconciliation: prefer TMDB-backed baseline when name-key baseline is empty
    # This handles scenarios where sync/watched returns limited data but sync/history has TMDB IDs
    if initial_watched_eps == 0 and len(start_episode_tmdb_snapshot) > 0:
        print(f"(TMDb-backed baseline: {len(start_episode_tmdb_snapshot):,} unique episodes seeded from history)")
    print(f"Starting movies in Trakt (snapshot): {initial_watched_movies:,}")

    # Coverage calculation with special handling for 0% scenarios
    # Prevents misleading percentage displays when baseline is empty
    if initial_watched_eps == 0:
        snapshot_tmdb_coverage_percent = 0.0
        print(f"Start snapshot coverage: {len(start_episode_tmdb_snapshot):,} TMDB IDs over 0 unique episodes (n/a)")
        if len(start_episode_tmdb_snapshot) > 0:
            # Key insight: TMDB baseline exists despite 0 name-key baseline
            # This indicates sync/watched returned minimal data but history hydration succeeded
            print("  Note: TMDB IDs seeded from history; Trakt's watched snapshot returned 0 episodes this run.")
    else:
        # Standard coverage calculation when name-key baseline exists
        snapshot_tmdb_coverage_percent = (len(start_episode_tmdb_snapshot) / initial_watched_eps) * 100.0
        print(f"Start snapshot coverage: {len(start_episode_tmdb_snapshot):,} TMDB IDs "
              f"over {initial_watched_eps:,} unique episodes ({snapshot_tmdb_coverage_percent:.1f}% TMDB coverage)")
    starting_unique_episodes_stat = None
    starting_movies_stat = None
    starting_shows_stat = None
    starting_total_plays_stat = None
    # Enhanced account verification with library stats
    username = traktIO.verifyAccountInfo()
    if username:
        print(f" Connected to Trakt account: {username}")
        
        # Get detailed library stats from the verification
        try:
            with Trakt.configuration.oauth.from_response(traktIO.authorization):
                stats = Trakt["users/me/stats"].get()
                if stats:
                    starting_shows_stat = stats.shows.watched
                    starting_unique_episodes_stat = stats.episodes.watched
                    starting_movies_stat = stats.movies.watched
                    starting_total_plays_stat = stats.episodes.plays + stats.movies.plays
                    print(" Your Trakt Library:")
                    print(f"   - {starting_shows_stat:,} shows watched")
                    print(f"   - {starting_unique_episodes_stat:,} episodes watched")
                    print(f"   - {starting_movies_stat:,} movies watched")
                    print(f"   - {starting_total_plays_stat:,} total plays")
        except Exception as e:
            logging.debug(f"Could not fetch detailed stats: {e}")
    else:
        print(" Could not verify Trakt account - check authentication")
    
    # Enhanced duplicate detection summary
    initial_watched_movies = len(traktIO._watched_movies)

    print("\n Duplicate Prevention:")
    if initial_watched_eps > 0:
        print(f"   - Cached {initial_watched_eps:,} watched episodes (name-key) for duplicate detection")
    else:
        print(f"   - Cached {len(start_episode_tmdb_snapshot):,} watched episodes (TMDb baseline) for duplicate detection")
    print(f"   - Cached {initial_watched_movies:,} watched movies for duplicate detection")
    print("   - Only new content will be imported to prevent duplicates")
    print()
    
    # Load Netflix history
    print("Loading Netflix viewing history...")
    netflixHistory = NetflixTvHistory()
    
    with open(config.VIEWING_HISTORY_FILENAME, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=config.CSV_DELIMITER)
        header_skipped = False
        for line in reader:
            if len(line) >= 2:
                title = line[0]
                date = line[1]
                
                # Skip CSV header row (typically "Title,Date")
                if not header_skipped:
                    if title.lower() == "title" and date.lower() == "date":
                        header_skipped = True
                        continue
                    header_skipped = True  # Handle files without explicit header
                
                netflixHistory.addEntry(title, date)
    
    # Post-processing - resolve ambiguous entries with context
    if hasattr(netflixHistory, 'ambiguous_entries') and netflixHistory.ambiguous_entries:
        # Determinism: sort ambiguous entries so resolution order is stable and testable
        try:
            def _amb_key(entry):
                # Prefer dictionary access for deterministic ordering
                if isinstance(entry, dict):
                    name = (entry.get('show_name') or entry.get('title') or entry.get('name') or '')
                    date = entry.get('date') or entry.get('watchedAt') or entry.get('watched_at') or ''
                else:
                    name = (
                        getattr(entry, 'show_name', None)
                        or getattr(entry, 'title', None)
                        or getattr(entry, 'name', None)
                        or str(entry)
                    )
                    date = (
                        getattr(entry, 'date', None)
                        or getattr(entry, 'watchedAt', None)
                        or getattr(entry, 'watched_at', None)
                        or ''
                    )
                return (str(name).lower(), str(date))
            netflixHistory.ambiguous_entries.sort(key=_amb_key)
        except Exception as sort_err:
            # Best-effort; keep going if structure is unexpected
            logging.debug(f"Ambiguous entry sort skipped due to structure: {sort_err}")

        initial_ambiguous_total = len(netflixHistory.ambiguous_entries)
        print(f"Resolving {initial_ambiguous_total} ambiguous entries with context...")
        netflixHistory.resolveAmbiguousEntries()
        
        # Log the classification results
        stats = getattr(netflixHistory, 'classification_stats', {}) or {}
        amb_resolved = int(stats.get('ambiguous_resolved', 0) or 0)
        amb_defaulted = int(
            stats.get('ambiguous_defaulted', max(0, initial_ambiguous_total - amb_resolved)) or 0
        )
        stats['ambiguous_total'] = initial_ambiguous_total
        stats['ambiguous_defaulted'] = amb_defaulted
        try:
            setattr(netflixHistory, 'classification_stats', stats)
        except Exception as store_err:
            logging.debug(
                f"Could not persist ambiguous classification stats: {store_err}"
            )

        print(
            f"  [OK] Ambiguous: {stats['ambiguous_total']}, resolved as episodes: {amb_resolved}, "
            f"defaulted to movies: {amb_defaulted}"
        )

        print(
            f"Inventory after resolution: {len(netflixHistory.shows)} TV shows, "
            f"{len(netflixHistory.movies)} movies"
        )
    
    # Initialize tracking for movies
    movies_added = 0
    movies_skipped = 0
    movies_not_found = 0
    total_movies_processed = 0
    
    # Process TV shows
    print("\nProcessing TV shows...")
    for show in tqdm(netflixHistory.shows, desc="Finding and adding shows to Trakt"):
        processShow(show, traktIO, tmdb_cache)
    
    # Process movies
    print("\nProcessing movies...")
    for movie in tqdm(netflixHistory.movies, desc="Finding and adding movies to Trakt"):
        total_movies_processed += 1
        result = processMovie(movie, traktIO, tmdb_cache)
        if result == "added":
            movies_added += 1
        elif result == "skipped":
            movies_skipped += 1
        else:  # "not_found"
            movies_not_found += 1
    
    # Log movie processing summary
    logging.info("=== MOVIE PROCESSING COMPLETE ===")
    logging.info(f"Total movies from Netflix: {total_movies_processed}")
    logging.info(f"Movies queued for Trakt sync: {movies_added}")
    logging.info(f"Movies skipped (already watched): {movies_skipped}")
    logging.info(f"Movies skipped (no TMDB ID): {movies_not_found}")
    
    # Log final episode statistics before sync
    logging.info("=== EPISODE PROCESSING COMPLETE ===")
    logging.info(f"Total episodes from Netflix: {total_netflix_episodes}")
    logging.info(f"Episodes after TMDB processing: {total_processed_episodes}")
    logging.info(f"Episodes queued for Trakt sync: {total_episodes_added}")
    logging.info(
        f"Episodes skipped (already watched snapshot): {skipped_preexisting_snapshot}"
    )
    logging.info(
        f"Episodes skipped (duplicate within run): {skipped_within_run_duplicates}"
    )
    logging.info(f"Episodes skipped (no TMDB ID): {total_episodes_skipped_no_tmdb}")
    
    # Verify our accounting adds up (all buckets cover distinct episodes)
    accounted_total_episodes = (
        len(queued_unique_episode_markers)
        + skipped_preexisting_snapshot
        + skipped_within_run_duplicates
        + total_episodes_skipped_no_tmdb
    )
    if accounted_total_episodes != total_processed_episodes:
        logging.error(
            "ACCOUNTING ERROR: Processed %s episodes but only accounted for %s",
            total_processed_episodes,
            accounted_total_episodes,
        )
    
    # Log TMDB cache efficiency summary before final sync
    tmdb_cache.log_summary()

    tmdb_marker_count = sum(
        1
        for marker in queued_unique_episode_markers
        if isinstance(marker, tuple) and marker and marker[0] == "tmdb"
    )
    slug_marker_count = max(0, len(queued_unique_episode_markers) - tmdb_marker_count)
    print(
        f"Unique-episode markers: {tmdb_marker_count:,} TMDB-backed, {slug_marker_count:,} title-derived"
    )
    if initial_watched_eps == 0 and len(start_episode_tmdb_snapshot) > 0:
        # we have a TMDb baseline; use it
        print(f"Expected unique new episodes from queue: {tmdb_marker_count:,} (TMDb-backed)")
    elif initial_watched_eps == 0:
        # truly no baseline at all
        print("Expected unique new episodes: unknown (no baseline)")
    else:
        print(f"Expected unique new episodes from queue: {tmdb_marker_count:,} (TMDb-backed)")

    # Perform the final sync to Trakt
    resp = syncToTrakt(
        traktIO,
        expected_episode_plays=total_episodes_added,
        movie_play_count=queued_movie_play_count,
        tmdb_marker_count=tmdb_marker_count,
        slug_marker_count=slug_marker_count,
        snapshot_tmdb_coverage=snapshot_tmdb_coverage_percent,
    )
    # Use API grounded numbers for summary (fallback to precomputed on failure)
    queued_episode_plays_actual = len(traktIO.getData().get("episodes", []))
    queued_movie_plays_actual = len(traktIO.getData().get("movies", []))
    queued_unique_episode_count = tmdb_marker_count + slug_marker_count
    queued_unique_movie_count = len(queued_unique_movie_ids)

    added_movies = 0
    added_episodes = total_episodes_added
    failed_movies = 0
    failed_episodes = 0
    duplicate_episode_plays_trakt = 0
    duplicate_movie_plays_trakt = 0
    if resp:
        added = resp.get("added", {})
        failed = resp.get("failed", {})
        raw_movies = added.get("movies", 0)
        raw_episodes = added.get("episodes", 0)
        added_movies = len(raw_movies) if isinstance(raw_movies, list) else int(raw_movies or 0)
        added_episodes = len(raw_episodes) if isinstance(raw_episodes, list) else int(raw_episodes or 0)
        failed_movies = int(failed.get("movies", 0) or 0)
        failed_episodes = int(failed.get("episodes", 0) or 0)
        duplicate_episode_plays_trakt = max(0, queued_episode_plays_actual - added_episodes - failed_episodes)
        duplicate_movie_plays_trakt = max(0, queued_movie_plays_actual - added_movies - failed_movies)
    
    print("\nQueue overview")
    print(f"  Netflix CSV episode entries processed: {total_processed_episodes:,}")

    # Critical distinction: Plays vs Unique Items
    # Plays = individual watch events (includes rewatches of same episode/movie)
    # Unique items = distinct episodes/movies (deduplicated count)
    episode_queue_line = f"  Episode plays queued: {queued_episode_plays_actual:,}"
    if queued_unique_episode_count:
        episode_queue_line += (
            f" across {tmdb_marker_count:,} unique new episodes (TMDB-backed)"
        )
        if slug_marker_count:
            episode_queue_line += (
                f" + {slug_marker_count:,} uncertain (title-derived only)"
            )
    print(episode_queue_line)

    # Movies follow same plays vs unique items distinction
    movie_queue_line = f"  Movie plays queued:   {queued_movie_plays_actual:,}"
    if queued_unique_movie_count:
        movie_queue_line += f" across {queued_unique_movie_count:,} unique movies"
    print(movie_queue_line)
    
    print("\nTrakt response")
    if resp:
        print(f"  Added: {added_episodes:,} episode plays, {added_movies:,} movie plays")
        print(
            f"  Rejected by Trakt as duplicate plays: {duplicate_episode_plays_trakt:,} episodes, "
            f"{duplicate_movie_plays_trakt:,} movies"
        )
        print(
            f"  API errors: {failed_episodes:,} episode plays, {failed_movies:,} movie plays"
        )
    else:
        print("  Sync failed - see log for details. Counts above reflect queued items only.")
    
    print("\nLocal filters (pre-queue)")
    print(
        f"  Episodes skipped locally (already in start snapshot): {skipped_preexisting_snapshot:,}"
    )
    print(
        f"  Episodes skipped locally (duplicate within this CSV run): {skipped_within_run_duplicates:,}"
    )
    print(f"  Episodes skipped (no TMDB match): {total_episodes_skipped_no_tmdb:,}")
    print(f"  Movies skipped locally (already watched): {movies_skipped:,}")
    print(f"  Movies skipped (no TMDB match): {movies_not_found:,}")
    
    # Snapshot reconciliation: intelligently choose baseline for post-sync calculations
    # Prefer name-key baseline when available, fall back to TMDB-backed baseline
    # This ensures accurate accounting even when sync/watched returns incomplete data
    effective_unique_eps_baseline = (
        initial_watched_eps if initial_watched_eps > 0 else len(start_episode_tmdb_snapshot)
    )

    print("\nSnapshot reconciliation (unique items)")
    print(f"  Starting unique episodes (snapshot): {initial_watched_eps:,}")
    if starting_unique_episodes_stat is not None:
        print(f"  Starting unique episodes (Trakt stats API): {starting_unique_episodes_stat:,}")

    # Indicate when TMDB-backed reconciliation is being used
    # This happens when sync/watched failed but history hydration provided TMDB baseline
    if initial_watched_eps == 0 and len(start_episode_tmdb_snapshot) > 0:
        print("  (Reconciling using TMDb-backed baseline this run)")

    # Report expected new episodes based on available baseline
    # Only TMDB-backed markers are counted as reliable for unique episode estimates
    if (initial_watched_eps > 0) or (len(start_episode_tmdb_snapshot) > 0):
        print(f"  Expected new episodes from queue: {tmdb_marker_count:,} (TMDb-backed)")
    else:
        print("  Expected new episodes from queue: unknown (no baseline)")

    # Calculate expected post-sync total using effective baseline
    # This provides accurate expectations regardless of which baseline was used
    print(f"  Expected post-sync unique episodes: {effective_unique_eps_baseline + tmdb_marker_count:,}")
    print(f"  Starting movies (snapshot): {initial_watched_movies:,}")
    if starting_movies_stat is not None:
        print(f"  Starting movies (Trakt stats API): {starting_movies_stat:,}")
    print(f"  Expected new movies from queue: {queued_unique_movie_count:,}")
    print(
        f"  Expected post-sync movie total: {initial_watched_movies + queued_unique_movie_count:,}"
    )
    if starting_shows_stat is not None:
        print(f"  Starting shows (Trakt stats API): {starting_shows_stat:,}")
    if starting_total_plays_stat is not None:
        print(f"  Starting total plays (Trakt stats API): {starting_total_plays_stat:,}")
    print("  (Verify updated totals on trakt.tv to confirm expectations.)")
    
    if resp:
        print("\n[OK] Processing complete. Review the log for detailed entries if needed.")
    else:
        print("\n[WARNING] Processing aborted during sync. Investigate the log before rerunning.")


if __name__ == "__main__":
    main()
