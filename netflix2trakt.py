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

import config
from NetflixTvShow import NetflixTvHistory, NetflixMovie, NetflixTvShowEpisode
from TraktIO import TraktIO

# Constants
EPISODES_AND_MOVIES_NOT_FOUND_FILE = "not_found.csv"

# Global tracking variables for comprehensive episode accounting
total_netflix_episodes = 0
total_processed_episodes = 0
total_episodes_added = 0
total_episodes_skipped_watched = 0
total_episodes_skipped_no_tmdb = 0

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
    These are common cases where Netflix exports differ from TMDB titles.
    """
    return {
        # Documentary series with missing subtitles
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
            
            # Second try: episode number in title
            if not matched:
                match = re.search(r"(?:Episode|Ep\.?)\s*(\d+)", episode.name, re.IGNORECASE)
                if match:
                    episode_number = int(match.group(1))
                    if season_data and "episodes" in season_data:
                        for tmdb_episode in season_data["episodes"]:
                            if tmdb_episode.get("episode_number") == episode_number:
                                episode_tmdb_id = tmdb_episode.get("id")
                                matched = True
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
                
                # Check if already watched
                if traktIO.isEpisodeWatched(show.name, season.number, episode_number):
                    logging.info(f"Episode already watched: {show.name} S{season.number}E{episode_number}")
                    total_episodes_skipped_watched += 1
                else:
                    # Add episode to Trakt queue
                    for watched_at in episode.watchedAt:
                        episode_data = {
                            "watched_at": watched_at,
                            "ids": {"tmdb": episode_tmdb_id}
                        }
                        traktIO.addEpisodeToHistory(episode_data)
                        logging.info(f"Adding episode: {show.name} S{season.number}E{episode_number}")
                    total_episodes_added += 1
            else:
                logging.warning(f"Episode not matched: {show.name} S{season.number} - {episode.name}")
                total_episodes_skipped_no_tmdb += 1
                append_not_found(show.name, season.number, episode.name)


def processMovie(movie: NetflixMovie, traktIO, tmdb_cache):
    """Process a movie and add to Trakt if found on TMDB"""
    # Get movie TMDB ID
    tmdb_id = getMovieInformationFromTMDB(movie.name, tmdb_cache)
    
    if tmdb_id:
        movie.tmdbId = tmdb_id
        
        # Check if already watched
        if traktIO.isMovieWatched(tmdb_id):
            logging.info(f"Movie already watched: {movie.name}")
            return "skipped"
        
        # Add movie to Trakt queue
        for watched_time in movie.watchedAt:
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


def syncToTrakt(traktIO, expected_episodes=None):
    """
    Final sync call to Trakt API with comprehensive logging and error handling
    """
    try:
        data_to_sync = traktIO.getData()
        
        movie_count = len(data_to_sync.get("movies", []))
        episode_count = len(data_to_sync.get("episodes", []))
        
        # Comprehensive logging for the sync phase
        logging.info("=== FINAL SYNC TO TRAKT ===")
        logging.info(f"Movies queued for sync: {movie_count}")
        logging.info(f"Episodes queued for sync: {episode_count}")
        logging.info(f"Total items queued for sync: {movie_count + episode_count}")
        
        # Validate that our queue matches our earlier counts
        if expected_episodes is not None and episode_count != expected_episodes:
            logging.error(f"SYNC QUEUE MISMATCH: Expected {expected_episodes} episodes in sync queue, found {episode_count}")
        
        print(f"\nSubmitting {movie_count} movies and {episode_count} episodes to Trakt...")
        print("Note: This may take several minutes due to rate limiting protection...")
        
        # Perform the sync
        response = traktIO.sync()
        
        if response:
            # Process response with enhanced error handling
            added = response.get("added", {})
            failed = response.get("failed", {})
            
            # Handle both integer and list response formats
            raw_movies = added.get("movies", 0)
            raw_episodes = added.get("episodes", 0)
            
            added_movies = len(raw_movies) if isinstance(raw_movies, list) else int(raw_movies)
            added_episodes = len(raw_episodes) if isinstance(raw_episodes, list) else int(raw_episodes)
            
            failed_movies = failed.get("movies", 0)
            failed_episodes = failed.get("episodes", 0)
            
            skipped_movies = movie_count - added_movies - failed_movies
            skipped_episodes = episode_count - added_episodes - failed_episodes
            
            # Display results
            print("\nTrakt sync complete!")
            print(f"Added: {added_movies} movies, {added_episodes} episodes")
            
            if skipped_movies > 0 or skipped_episodes > 0:
                print(f"Skipped (already watched): {skipped_movies} movies, {skipped_episodes} episodes")
            
            if failed_movies > 0 or failed_episodes > 0:
                print(f"FAILED (API errors): {failed_movies} movies, {failed_episodes} episodes")
                print("Check the log file for details on failed items.")
                
                # Check for failed items
                failed_items = traktIO.get_failed_items()
                if failed_items["episodes"]:
                    logging.error(f"Failed episodes: {len(failed_items['episodes'])} items")
                if failed_items["movies"]:
                    logging.error(f"Failed movies: {len(failed_items['movies'])} items")
                
                print("\n⚠️  Some items failed to sync. You may want to try again later.")
                print("   The failed items have been logged for potential retry.")
            
            # Record any Trakt not_found items
            nf = response.get("not_found", {})
            if isinstance(nf, dict):
                for m in nf.get("movies", []) or []:
                    title = m.get("title") if isinstance(m, dict) else None
                    append_not_found(title or "UNKNOWN_MOVIE")
                for s in nf.get("shows", []) or []:
                    title = s.get("title") if isinstance(s, dict) else None
                    append_not_found(title or "UNKNOWN_SHOW")
    
    except Exception as e:
        print(f"Trakt sync failed with exception: {e}")
        logging.error(f"Trakt sync failed with exception: {e}", exc_info=True)


def main():
    """Entry point: loads config, parses history, syncs Trakt"""
    
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
    
    # Configure Trakt
    traktIO = TraktIO(
        page_size=config.TRAKT_API_SYNC_PAGE_SIZE,
        dry_run=config.TRAKT_API_DRY_RUN
    )
    
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
    
    # NEW: Post-processing - resolve ambiguous entries with context
    if hasattr(netflixHistory, 'ambiguous_entries') and netflixHistory.ambiguous_entries:
        print(f"Resolving {len(netflixHistory.ambiguous_entries)} ambiguous entries with context...")
        netflixHistory.resolveAmbiguousEntries()
        
        # Log the classification results
        stats = getattr(netflixHistory, 'classification_stats', {})
        resolved_count = stats.get('ambiguous_resolved', 0)
        if resolved_count > 0:
            print(f"  ✅ Resolved {resolved_count} ambiguous entries using context analysis")
    
    print(f"Found {len(netflixHistory.shows)} TV shows and {len(netflixHistory.movies)} movies")
    
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
    logging.info(f"Episodes skipped (already watched): {total_episodes_skipped_watched}")
    logging.info(f"Episodes skipped (no TMDB ID): {total_episodes_skipped_no_tmdb}")
    
    # Verify our accounting adds up
    accounted_total = total_episodes_added + total_episodes_skipped_watched + total_episodes_skipped_no_tmdb
    if accounted_total != total_processed_episodes:
        logging.error(f"ACCOUNTING ERROR: Processed {total_processed_episodes} episodes but only accounted for {accounted_total}")
    
    # Log TMDB cache efficiency summary before final sync
    tmdb_cache.log_summary()
    
    # Perform the final sync to Trakt
    syncToTrakt(traktIO, total_episodes_added)
    
    print("\n✅ Processing complete! Check the log file for detailed information.")


if __name__ == "__main__":
    main()