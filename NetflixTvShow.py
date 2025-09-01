import datetime
import logging
import re
from typing import Optional, Set, Union

import config

# Setup logging
logging.basicConfig(filename=config.LOG_FILENAME, level=config.LOG_LEVEL)


# A class that stores all the shows and movies that you have watched on Netflix.
class NetflixTvHistory(object):
    def __init__(self):
        self.shows = []
        self.movies = []
        
        # NEW: Enhanced episode classification system
        self.known_show_names = set()  # Track confirmed TV show names for context
        self.ambiguous_entries = []    # Store Show: Title entries for post-processing
        self.classification_stats = {
            'episodes_certain': 0,      # Episodes with clear S#E# format
            'episodes_inferred': 0,     # Episodes classified via context/patterns
            'movies_certain': 0,        # Clear movie entries
            'ambiguous_resolved': 0     # Ambiguous entries resolved via context
        }

    def hasTvShow(self, showName: str) -> bool:
        """
        Returns `True` if the show exists in the database, `False` otherwise
        :param showName: The name of the show you want to check for
        :type showName: str
        :return: A boolean value.
        """
        return self.getTvShow(showName) is not None

    def getTvShow(self, showName: str):
        """
        Get the tv show with name showName
        :param showName:
        :return the found tv show or None:
        """
        for show in self.shows:
            if show.name == showName:
                return show
        return None

    def getMovie(self, movieName: str):
        """
        It returns the movie object with the name movieName
        :param movieName: The name of the movie you want to get
        :type movieName: str
        :return: A movie object
        """
        for movie in self.movies:
            if movie.name == movieName:
                return movie
        return None

    def addEntry(self, entryTitle: str, entryDate: str) -> bool:
        """
        It takes a string and tries to find a pattern that matches a TV show. If it finds one, it adds
        the TV show to the history list. If it doesn't find one, it adds the string as a movie

        :param entryTitle: The title of the entry
        :type entryTitle: str
        :param entryDate: The date the entry was watched
        :type entryDate: str
        :return: A list of tuples.
        """

        # check for a pattern TvShow : Season 1: EpisodeTitle
        tvshowregex = re.compile(r"(.+): .+ (\d{1,2}): (.*)")
        res = tvshowregex.search(entryTitle)
        if res is not None:
            # found tv show
            showName = res.group(1)
            seasonNumber = int(res.group(2))
            episodeTitle = res.group(3)
            self.addTvShowEntry(showName, seasonNumber, episodeTitle, entryDate)
            return True

        # Check for TvShow : Season 1 - Part A: EpisodeTitle
        # Example: Die außergewoehnlichsten Haeuser der Welt: Staffel 2 – Teil B: Spanien
        tvshowregex = re.compile(r"(.+): .+ (\d{1,2}) – .+: (.*)")
        res = tvshowregex.search(entryTitle)
        if res is not None:
            # found tv show
            showName = res.group(1)
            seasonNumber = int(res.group(2))
            episodeTitle = res.group(3)
            self.addTvShowEntry(showName, seasonNumber, episodeTitle, entryDate)
            return True

        # Check for TvShow : TvShow: Miniseries : EpisodeTitle
        tvshowregex = re.compile(r"(.+): \w+: (.+)")
        res = tvshowregex.search(entryTitle)
        if res is not None:
            showName = res.group(1)
            seasonNumber = 1
            episodeTitle = res.group(2)
            self.addTvShowEntry(showName, seasonNumber, episodeTitle, entryDate)
            return True

        # Check for TvShow: SeasonName : EpisodeTitle
        # Example: American Horror Story: Murder House: Nachgeburt
        tvshowregex = re.compile(r"(.+): (.+): (.+)")
        res = tvshowregex.search(entryTitle)
        if res is not None:
            showName = res.group(1)
            seasonName = res.group(2)
            episodeTitle = res.group(3)
            self.addTvShowEntry(
                showName, None, episodeTitle, entryDate, seasonName=seasonName
            )
            return True

        # Check for TvShow: EpisodeTitle (ENHANCED with context awareness)
        # sometimes used in this format for the first season of a show
        # Example: "Wednesday: Leid pro quo","29.11.22"
        # @tricky: Also movies sometimes use this format (e.g "King Arthur: Legend of the Sword","17.01.21")
        tvshowregex = re.compile(r"(.+): (.+)")
        res = tvshowregex.search(entryTitle)
        if res is not None:
            showName = res.group(1)
            episodeTitle = res.group(2)
            
            # NEW: Enhanced classification logic to reduce misclassification
            classification = self.isLikelyTvShow(showName, episodeTitle)
            
            if classification is True:
                # Confident it's a TV show episode - process as episode only
                self.addTvShowEntry(showName, 1, episodeTitle, entryDate)
                logging.debug(f"Episode classification: Classified '{entryTitle}' as TV episode (confident)")
                return True
            elif classification is False:
                # Confident it's a movie - skip to movie processing below
                logging.debug(f"Episode classification: Classified '{entryTitle}' as movie (confident)")
                pass  # Continue to movie processing below
            else:
                # Uncertain - store for later context-based resolution
                logging.debug(f"Episode classification: Storing '{entryTitle}' as ambiguous for later resolution")
                self.ambiguous_entries.append({
                    'title': entryTitle,
                    'show_name': showName,
                    'episode_title': episodeTitle,
                    'date': entryDate
                })
                return True  # Don't process as movie yet - wait for context resolution

        # Else the entry is a movie (UNCHANGED)
        self.addMovieEntry(entryTitle, entryDate)
        return True

    def addTvShowEntry(
        self,
        showName: str,
        seasonNumber: Union[int, None],
        episodeTitle: str,
        watchedDate: str,
        seasonName: Optional[str] = None,
    ) -> None:
        """
        It adds a TV show, season, episode, and watched date to the history list

        :param showName: The name of the show
        :type showName: str
        :param seasonNumber: The season number
        :type seasonNumber: Union[int,None]
        :param episodeTitle: The title of the episode
        :type episodeTitle: str
        :param watchedDate: The date the episode was watched
        :type watchedDate: str
        :param seasonName: The name of the season, if it has one
        :type seasonName: Optional[str]
        """
        # Validate date before creating show/season/episode
        if not watchedDate or watchedDate.lower() in ("date", "datum"):
            import logging
            logging.warning(f"Skipping TV show entry '{showName}' due to invalid date: '{watchedDate}'")
            return
            
        if not any(char.isdigit() for char in watchedDate):
            import logging
            logging.warning(f"Skipping TV show entry '{showName}' due to non-date value: '{watchedDate}'")
            return
        
        show = self.addTvShow(showName)
        season = show.addSeason(seasonNumber=seasonNumber, seasonName=seasonName)
        episode = season.addEpisode(episodeTitle)
        episode.addWatchedDate(watchedDate)
        
        # NEW: Track confirmed show names for enhanced episode classification
        self.known_show_names.add(showName)
        
        # NEW: Update classification statistics
        if seasonNumber is not None or seasonName is not None:
            self.classification_stats['episodes_certain'] += 1
        else:
            self.classification_stats['episodes_inferred'] += 1

    def addTvShow(self, showName):
        """
        It adds a tv show to the list of shows.

        :param showName: The name of the show you want to add
        :return: The last item in the list of shows.
        """
        show = self.getTvShow(showName)
        if show is not None:
            return show
        else:
            self.shows.append(NetflixTvShow(showName))
            return self.shows[-1]

    def addMovieEntry(self, movieTitle, watchedDate):
        """
        It adds a movie to the list of movies and adds the date it was watched to the movie.

        :param movieTitle: The title of the movie you want to add
        :param watchedDate: a string in the format "YYYY-MM-DD" or similar (as given in the config)
        :return: The movie object that was just added to the list of movies.
        """
        # Validate date before creating movie
        if not watchedDate or watchedDate.lower() in ("date", "datum"):
            import logging
            logging.warning(f"Skipping movie '{movieTitle}' due to invalid date: '{watchedDate}'")
            return None
            
        if not any(char.isdigit() for char in watchedDate):
            import logging
            logging.warning(f"Skipping movie '{movieTitle}' due to non-date value: '{watchedDate}'")
            return None
        
        movie = self.getMovie(movieTitle)
        if movie is not None:
            movie.addWatchedDate(watchedDate)
            return movie
        else:
            self.movies.append(NetflixMovie(movieTitle))
            movie = self.movies[-1]
            result = movie.addWatchedDate(watchedDate)
            # If date parsing failed, remove the movie
            if result is False:
                self.movies.pop()
                return None
            return movie

    # NEW: Enhanced episode classification methods for fixing misclassified Show: Title entries
    
    def isLikelyTvShow(self, showName: str, episodeTitle: str) -> bool:
        """
        Enhanced logic to determine if Show: Title pattern is likely TV episode vs movie.
        
        :param showName: The show name part (before the colon)
        :param episodeTitle: The episode title part (after the colon)
        :return: True if likely TV show, False if likely movie, None if uncertain
        """
        import re
        
        # Check if show name already exists in our confirmed show registry
        if showName in self.known_show_names:
            logging.debug(f"Episode classification: Known show detected: {showName}")
            return True
        
        # Check for clear episode indicators in the title
        episode_patterns = [
            r'Episode \d+', r'Part \d+', r'Chapter \d+', r'Act \d+',
            r'Season Finale', r'Pilot', r'Finale', r'Premiere'
        ]
        
        for pattern in episode_patterns:
            if re.search(pattern, episodeTitle, re.IGNORECASE):
                logging.debug(f"Episode classification: Episode pattern found in '{episodeTitle}': {pattern}")
                return True
        
        # Check for clear movie indicators (less likely to be episodes)
        movie_patterns = [
            r'Legend of', r'Rise of', r'Return of', r'Age of',
            r'The .+ Movie', r'Director.*Cut', r'Extended Edition',
            r'Special Edition', r'Uncut', r'Remastered'
        ]
        
        full_title = f"{showName}: {episodeTitle}"
        for pattern in movie_patterns:
            if re.search(pattern, full_title, re.IGNORECASE):
                logging.debug(f"Episode classification: Movie pattern found in '{full_title}': {pattern}")
                return False
        
        # No clear indicators - mark as uncertain for later context-based resolution
        logging.debug(f"Episode classification: Uncertain classification for '{showName}: {episodeTitle}'")
        return None  # Explicitly uncertain
    
    def resolveAmbiguousEntries(self):
        """
        Post-process ambiguous Show: Title entries using accumulated context from known TV shows.
        This is called after all Netflix entries have been initially parsed.
        """
        resolved_count = 0
        
        # NEW: Build frequency map of show names from ambiguous entries
        show_frequencies = {}
        for entry in self.ambiguous_entries:
            show_name = entry['show_name']
            if show_name not in show_frequencies:
                show_frequencies[show_name] = []
            show_frequencies[show_name].append(entry)
        
        for entry in self.ambiguous_entries:
            show_name = entry['show_name']
            
            # Method 1: Show name was confirmed elsewhere during parsing
            if show_name in self.known_show_names:
                logging.info(f"Episode classification: Resolving ambiguous entry as episode (known show): {show_name}")
                self.addTvShowEntry(
                    show_name, 1, 
                    entry['episode_title'], 
                    entry['date']
                )
                self.classification_stats['ambiguous_resolved'] += 1
                resolved_count += 1
                
            # Method 2: Multiple episodes from same show suggest it's a TV series
            elif len(show_frequencies[show_name]) >= 2:
                logging.info(f"Episode classification: Resolving ambiguous entry as episode (multiple episodes): {show_name}")
                self.addTvShowEntry(
                    show_name, 1, 
                    entry['episode_title'], 
                    entry['date']
                )
                self.classification_stats['ambiguous_resolved'] += 1
                resolved_count += 1
                
            else:
                # No additional context found - classify as movie (preserve original behavior)
                logging.info(f"Episode classification: Resolving ambiguous entry as movie: {entry['title']}")
                self.addMovieEntry(entry['title'], entry['date'])
        
        if resolved_count > 0:
            logging.info(f"Episode classification: Resolved {resolved_count} ambiguous entries as episodes based on context")
        
        # Clear the list after processing
        self.ambiguous_entries.clear()

    def getJson(self) -> dict:
        """
        It takes the data from the objects and puts it into a dictionary
        :return: A dictionary with two keys, "tvshows" and "movies".
        """
        jsonOut: dict = {"tvshows": {}, "movies": {}}
        for show in self.shows:
            jsonOut["tvshows"][show.name] = []
            for season in show.seasons:
                jsonOut["tvshows"][show.name].append(
                    {
                        "SeasonNumber": season.number,
                        "SeasonName": season.name,
                        "episodes": {},
                    }
                )
                for episode in season.episodes:
                    jsonOut["tvshows"][show.name][-1]["episodes"][episode.name] = []
                    for watchedAt in episode.watchedAt:
                        jsonOut["tvshows"][show.name][-1]["episodes"][
                            episode.name
                        ].append(watchedAt)
        for movie in self.movies:
            jsonOut["movies"][movie.name] = []
            for watchedAt in movie.watchedAt:
                jsonOut["movies"][movie.name].append(watchedAt)
        return jsonOut


# A class that represents a Netflix watchable item
class NetflixWatchableItem(object):
    def __init__(self, name: str):
        self.name = name
        # watchedAt is a set to prevent duplicate entries
        self._watchedAt: Set[str] = set()

    @property
    def watchedAt(self):
        return list(self._watchedAt)

    def addWatchedDate(self, watchedDate: str):
        try:
            # Netflix exports only have the date. Add an arbitrary time.
            time = datetime.datetime.strptime(
                watchedDate + " 20:15", config.CSV_DATETIME_FORMAT + " %H:%M"
            )
        except ValueError:
            try:
                # try the date with a dot (also for backwards compatbility)
                watchedDate = re.sub("[^0-9]", ".", watchedDate)
                time = datetime.datetime.strptime(watchedDate + " 20:15", "%m.%d.%y %H:%M")
            except ValueError as e:
                import logging
                logging.error(f"Failed to parse date '{watchedDate}': {e}")
                return False
        formatted_date = time.strftime("%Y-%m-%dT%H:%M:%S.00Z")
        self._watchedAt.add(formatted_date)
        return True


# The `NetflixMovie` class is a subclass of the `NetflixWatchableItem` class
class NetflixMovie(NetflixWatchableItem):
    def __init__(self, movieName: str):
        super().__init__(movieName)
        self.tmdbId = None


# `NetflixTvShowEpisode` is a `NetflixWatchableItem` that has a `tmdbId` and a `number`
class NetflixTvShowEpisode(NetflixWatchableItem):
    def __init__(self, episodeName: str):
        super().__init__(episodeName)
        self.tmdbId = None
        self.number: Union[int, None] = None

    def setEpisodeNumber(self, number: Union[int, None]):
        """
        Sets the episode number

        :param number: The episode number
        :type number: Union[int, None]
        """
        self.number = number

    def setTmdbId(self, id):
        """
        Sets the tmdbId of the episode
        :param id: The ID of the episode
        """
        self.tmdbId = id


# A NetflixTvShowSeason is a season of a tv show, and it has a number, a name, and a list of episodes
class NetflixTvShowSeason(object):
    def __init__(self, seasonNumber: int, seasonName: Optional[str] = None):
        self.number: int = seasonNumber
        self.name: Optional[str] = seasonName
        self.episodes: list[NetflixTvShowEpisode] = []

    def addEpisode(self, episodeName: str):
        """
        If the episode already exists, return it. Otherwise, add it to the list of episodes and return it

        :param episodeName: The name of the episode
        :type episodeName: str
        :return: The last episode in the list of episodes.
        """
        episode = self.getEpisodeByName(episodeName)
        if episode is not None:
            return episode

        self.episodes.append(NetflixTvShowEpisode(episodeName))
        return self.episodes[-1]

    def getEpisodeByName(self, episodeName: str):
        """
        Returns the episode with the given name.

        :param episodeName: The name of the episode you want to get
        :type episodeName: str
        :return: The episode if it was found, otherwise None
        """
        for episode in self.episodes:
            if episode.name == episodeName:
                return episode
        return None


# The NetflixTvShow class represents a TV show on Netflix. It has a name, and a list of seasons
class NetflixTvShow(object):
    def __init__(self, showName: str):
        self.name: str = showName
        self.seasons: list[NetflixTvShowSeason] = []

    def addSeason(self, seasonNumber: Union[int, None], seasonName: Optional[str] = None) -> NetflixTvShowSeason:
        """
        If the season doesn't exist, add it to the list of seasons

        :param seasonNumber: The season number of the season (None defaults to 1)
        :type seasonNumber: Union[int, None]
        :param seasonName: The name of the season
        :type seasonName: Optional[str]
        :return: The last season added to the list of seasons
        """
        # Handle None season number (default to 1 for limited series)
        effective_season_number = seasonNumber if seasonNumber is not None else 1
        
        season = self.getSeasonByNumber(effective_season_number)
        if season is not None:
            return season
        season = self.getSeasonByName(seasonName)
        if season is not None:
            return season

        self.seasons.append(NetflixTvShowSeason(effective_season_number, seasonName))
        return self.seasons[-1]

    def getSeasonByNumber(self, seasonNumber: int) -> Union[NetflixTvShowSeason, None]:
        """
        "Return the season with the given number, or None if no such season exists."

        :param seasonNumber: The season number you want to get
        :type seasonNumber: int
        :return: A NetflixTvShowSeason object or None
        """
        for season in self.seasons:
            if season.number == seasonNumber:
                return season
        return None

    def getSeasonByName(self, seasonName: str) -> Union[NetflixTvShowSeason, None]:
        """
        Returns a season object from the seasons list if the name of the season matches the
        name passed to the function, otherwise none

        :param seasonName: The name of the season you want to get
        :type seasonName: str
        :return: A NetflixTvShowSeason object or None
        """
        for season in self.seasons:
            if season.name == seasonName and season.name is not None:
                return season
        return None
