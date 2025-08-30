#!/usr/bin/env python3

import csv
import os
import json
import logging
import config

from tenacity import retry, stop_after_attempt, wait_random
from tmdbv3api import TV, Movie, Season, TMDb
from tmdbv3api.exceptions import TMDbException
from tqdm import tqdm
from NetflixTvShow import NetflixTvHistory
from TraktIO import TraktIO
from csv import writer as csv_writer

EPISODES_AND_MOVIES_NOT_FOUND_FILE = "not_found.csv"

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
    # Handles caching of TMDB query results to reduce API usage
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
        key = title.lower()
        if key in self.cache:
            self.hits += 1
            return self.cache[key]
        else:
            self.misses += 1
            return None

    def set_cached_result(self, title, result):
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
        """Reduce TMDB object to a JSON-serializable plain dict with key fields.

        We keep only primitive / list / dict values and a short subset to control size.
        """
        # TMDb objects often behave like dicts
        if isinstance(result, dict):
            base = result
        else:
            # Try to coerce via attributes
            base = getattr(result, "__dict__", {})
        allowed_keys = [
            "id", "name", "title", "original_name", "original_title",
            "first_air_date", "release_date", "media_type"
        ]
        out = {}
        for k in allowed_keys:
            if k in base:
                v = base[k]
                if isinstance(v, (str, int, float, type(None))):
                    out[k] = v
        # Always include id if available
        if "id" not in out and hasattr(result, "id"):
            try:
                out["id"] = int(getattr(result, "id"))
            except Exception as id_err:
                logging.debug(f"Could not coerce TMDB result id: {id_err}")
        return out

    def log_summary(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100.0) if total else 0.0
        logging.info(
            f"TMDB cache summary: hits={self.hits} misses={self.misses} hit_rate={hit_rate:.1f}% entries={len(self.cache)}"
        )


def setupTMDB(tmdbKey, tmdbLanguage, tmdbDebug):
    # Configures and returns the TMDb API client
    tmdb = TMDb()
    tmdb.api_key = tmdbKey
    tmdb.language = tmdbLanguage
    tmdb.debug = tmdbDebug
    return tmdb


def setupTrakt(traktPageSize, traktDryRun):
    # Initializes the TraktIO interface
    traktIO = TraktIO(page_size=traktPageSize, dry_run=traktDryRun)
    return traktIO


def getNetflixHistory(inputFile, inputFileDelimiter):
    """
    Parses Netflix viewing history in CSV format.

    :param inputFile: File containing Netflix viewing history
    :param inputFileDelimiter: Delimiter used in Netflix viewing history (ex. CSV = `,`)
    :return: Returns `netflixHistory` that contains information parsed from viewing history CSV
    """
    # Load Netflix Viewing History and loop through every entry
    netflixHistory = NetflixTvHistory()
    with open(inputFile, mode="r", encoding="utf-8") as csvFile:
        # Make sure the file has a header "Title, Date" as the first line (Netflix export); we skip that header row below.
        csvReader = csv.DictReader(
            csvFile, fieldnames=("Title", "Date"), delimiter=inputFileDelimiter
        )
        line_count = 0
        for row in csvReader:
            if line_count == 0:
                line_count += 1
                continue

            entry = row["Title"]
            watchedAt = row["Date"]
            logging.debug("Parsed CSV file entry: {} : {}".format(watchedAt, entry))
            netflixHistory.addEntry(entry, watchedAt)
            line_count += 1
        logging.info(f"Processed {line_count} lines.")
    return netflixHistory


def dump_uncategorized_titles(submitted_titles, response, label="sync"):
    # Logs any titles submitted that were not recognized by Trakt
    if not response:
        print("\nNo response from Trakt to categorize titles.")
        return

    # If the TraktIO.sync result contains only integer counts (library aggregate response),
    # we cannot reliably map individual submitted titles to added/updated/not_found buckets.
    # In that case, skip the 'uncategorized' warning to avoid noisy UNKNOWN_EPISODE lines.
    added_section = response.get("added", {})
    if any(isinstance(added_section.get(k), int) for k in ("movies", "episodes", "shows")):
        logging.debug("Skipping uncategorized title analysis: response has aggregate counts only.")
        return

    known_titles = set()
    for category in ["added", "updated", "not_found"]:
        section = response.get(category, {})
        if isinstance(section, dict):
            for kind in ["movies", "episodes", "shows"]:
                entries = section.get(kind)
                if isinstance(entries, list):
                    for entry in entries:
                        title = (
                            entry.get("title")
                            or entry.get("show", {}).get("title")
                            or "UNKNOWN"
                        )
                        known_titles.add(title.lower().strip())

    unknown = [t for t in submitted_titles if t.lower().strip() not in known_titles]
    if unknown:
        print(f"\n{len(unknown)} titles not acknowledged in Trakt response:")
        for title in unknown:
            print("  -", title)
        with open(f"uncategorized_{label}.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Unacknowledged Titles"])
            for title in unknown:
                writer.writerow([title])


@retry(stop=stop_after_attempt(5), wait=wait_random(min=2, max=10))
def getShowInformation(
    show, languageSearch, traktIO, tmdb_cache: TMDBHelper, tmdbTv=None
):
    # Fetches show and episode information from TMDB, adds to Trakt
    if tmdbTv is None:
        tmdbTv = TV()
    tmdbSeason = Season()
    tmdbShow = None

    if len(show.name.strip()) == 0:
        return

    try:
        cached_result = tmdb_cache.get_cached_result(show.name)
        if cached_result:
            tmdbShow = cached_result
            logging.debug(f"Cache hit for show: {show.name}")
        else:
            search_results = tmdbTv.search(show.name)
            if search_results:
                tmdbShow = search_results[0]
                tmdb_cache.set_cached_result(show.name, tmdbShow)
                logging.debug(f"Cache miss; queried TMDB for: {show.name}")
    except Exception as e:
        logging.warning("TMDB query failed for show: %s (%s)" % (show.name, e))
        return

    if not tmdbShow:
        logging.warning("Show %s not found on TMDB." % show.name)
        append_not_found(show.name)
        return

    showId = tmdbShow.get("id")
    if not showId:
        logging.warning("Could not get TMDB ID for show: %s" % show.name)
        return

    for season in show.seasons:
        tmdbResult = None
        try:
            tmdbResult = tmdbSeason.details(tv_id=showId, season_num=season.number)
        except TMDbException as e:
            logging.error(f"Error fetching season details: {e}")
            continue

        if tmdbResult and hasattr(tmdbResult, "episodes"):
            for episode in season.episodes:
                tmdb_episodes = getattr(tmdbResult, "episodes", [])

                # Primary: match by episode title (case-insensitive)
                if hasattr(episode, 'name') and episode.name:
                    name_lower = episode.name.lower()
                    for tmdbEpisode in tmdb_episodes:
                        if tmdbEpisode.get("name", "").lower() == name_lower:
                            episode.tmdbId = tmdbEpisode.get("id")
                            logging.debug(
                                f"Matched episode {show.name} S{season.number} '{episode.name}' by title with TMDB ID {episode.tmdbId}"
                            )
                            break

                # (Optional future heuristic: add fuzzy/partial matching here if needed)

                # Secondary: episode number match (if provided and still unmatched)
                if not episode.tmdbId and episode.number is not None:
                    for tmdbEpisode in tmdb_episodes:
                        if tmdbEpisode.get("episode_number") == episode.number:
                            episode.tmdbId = tmdbEpisode.get("id")
                            logging.debug(
                                f"Matched episode {show.name} S{season.number}E{episode.number} by number with TMDB ID {episode.tmdbId}"
                            )
                            break

                if not episode.tmdbId:
                    logging.warning(
                        f"Could not find TMDB match for episode {show.name} S{season.number} '{getattr(episode,'name','UNKNOWN')}'"
                    )
                    append_not_found(show.name, season.number, getattr(episode, 'name', episode.number))
        else:
            logging.warning(
                f"Could not retrieve episodes for {show.name} season {season.number}"
            )

    addShowToTrakt(show, traktIO)


def getMovieInformation(movie, strictSync, traktIO, tmdb_cache: TMDBHelper):
    # Fetches TMDB info for a movie and adds to Trakt
    tmdbMovie = Movie()
    try:
        res = tmdb_cache.get_cached_result(movie.name)
        if not res:
            search_results = tmdbMovie.search(movie.name)
            if search_results:
                res = search_results[0]
                tmdb_cache.set_cached_result(movie.name, res)

        if res:
            movie.tmdbId = res.get("id")
            logging.info(
                "Found movie %s : %s (%s)"
                % (movie.name, res.get("title"), movie.tmdbId)
            )
            addMovieToTrakt(movie, traktIO)
        else:
            logging.info("Movie not found: %s" % movie.name)
            append_not_found(movie.name)
    except TMDbException as e:
        if strictSync:
            raise
        else:
            logging.info(
                "Ignoring exception while looking for movie %s: %s" % (movie.name, e)
            )


def addShowToTrakt(show, traktIO):
    # Adds each episode from a show to Trakt
    for season in show.seasons:
        logging.info(
            f"Adding episodes to trakt: {len(season.episodes)} episodes from {show.name} season {season.number}"
        )
        for episode in season.episodes:
            if not episode.tmdbId:
                continue
            # Only check watched cache if we have an episode number (some parsing paths may not set one)
            if episode.number is not None and traktIO.isEpisodeWatched(show.name, season.number, episode.number):
                logging.debug(
                    f"Skipping already-watched episode: {show.name} S{season.number}E{episode.number}"
                )
                continue
            for watchedTime in episode.watchedAt:
                episodeData = {
                    "watched_at": watchedTime,
                    "ids": {"tmdb": episode.tmdbId},
                }
                traktIO.addEpisodeToHistory(episodeData)


def addMovieToTrakt(movie, traktIO):
    # Adds a movie to Trakt
    if movie.tmdbId:
        if traktIO.isWatchedMovie(movie.tmdbId):
            logging.debug(f"Skipping already-watched movie: {movie.name}")
            return

        for watchedTime in movie.watchedAt:
            logging.info("Adding movie to trakt: %s" % movie.name)
            movieData = {
                "title": movie.name,
                "watched_at": watchedTime,
                "ids": {"tmdb": movie.tmdbId},
            }
            traktIO.addMovie(movieData)


def syncToTrakt(traktIO):
    # Final sync call to Trakt API
    try:
        logged_titles = []
        data_to_sync = traktIO.getData()

        movie_titles = [e.get("title", "UNKNOWN_MOVIE") for e in data_to_sync.get("movies", [])]
        episode_titles = [e.get("title", "UNKNOWN_EPISODE") for e in data_to_sync.get("episodes", [])]

        logged_titles.extend(movie_titles)
        logged_titles.extend(episode_titles)

        print(f"Submitting {len(movie_titles)} movies and {len(episode_titles)} episodes (total {len(logged_titles)} items) to Trakt ...")

        response = traktIO.sync()
        if response:
            dump_uncategorized_titles(logged_titles, response, label="sync")

            added = response.get("added", {})

            # Handle both integer and list response formats
            raw_movies = added.get("movies", 0)
            raw_episodes = added.get("episodes", 0)

            added_movies = len(raw_movies) if isinstance(raw_movies, list) else int(raw_movies)
            added_episodes = len(raw_episodes) if isinstance(raw_episodes, list) else int(raw_episodes)

            skipped_movies = len(movie_titles) - added_movies
            skipped_episodes = len(episode_titles) - added_episodes

            print(f"Trakt sync complete. Added: {added_movies} movies, {added_episodes} episodes.")
            print(f"Skipped (already watched / duplicate): {skipped_movies} movies, {skipped_episodes} episodes.")

            # Record any Trakt not_found (movies/shows) for diagnostics
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


def main():
    # Entry point: loads config, parses history, syncs Trakt
    # FIX: Use correct constant name EPISODES_AND_MOVIES_NOT_FOUND_FILE (defined on line 17)
    with open(EPISODES_AND_MOVIES_NOT_FOUND_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv_writer(f)
        writer.writerow(["Show", "Season", "Episode"])

    logging.basicConfig(filename=config.LOG_FILENAME, level=config.LOG_LEVEL)

    # Configure TMDB
    tmdb = setupTMDB(config.TMDB_API_KEY, config.TMDB_LANGUAGE, config.TMDB_DEBUG)
    tmdb_cache = TMDBHelper()

    traktIO = setupTrakt(config.TRAKT_API_SYNC_PAGE_SIZE, config.TRAKT_API_DRY_RUN)

    netflixHistory = getNetflixHistory(
        config.VIEWING_HISTORY_FILENAME, config.CSV_DELIMITER
    )

    tv = TV()
    for show in tqdm(netflixHistory.shows, desc="Finding and adding shows to Trakt.."):
        getShowInformation(
            show, config.TMDB_EPISODE_LANGUAGE_SEARCH, traktIO, tmdb_cache, tv
        )

    for movie in tqdm(
        netflixHistory.movies, desc="Finding and adding movies to Trakt.."
    ):
        getMovieInformation(movie, config.TMDB_SYNC_STRICT, traktIO, tmdb_cache)

    # Log TMDB cache efficiency summary before final sync
    tmdb_cache.log_summary()

    syncToTrakt(traktIO)


if __name__ == "__main__":
    main()
