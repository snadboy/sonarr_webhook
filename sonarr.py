import logging
import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import aiohttp
from dotenv import load_dotenv
from urllib.parse import urljoin
from sonarr_cache import SonarrCache

class SonarrError(Exception):
    """Base exception for Sonarr API errors"""
    pass

class Sonarr:
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, log_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        """
        Initialize Sonarr client
        
        Args:
            api_key (Optional[str]): Sonarr API key (default: from env SONARR_API_KEY)
            base_url (Optional[str]): Sonarr base URL (default: from env SONARR_URL)
            log_level (int): Logging level (default: logging.INFO)
            logger (Optional[logging.Logger]): Custom logger instance
        """
        # Setup logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                handler.setLevel(log_level)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        
        self.logger.setLevel(log_level)
        
        # Load environment variables if needed
        load_dotenv()
        
        # Initialize Sonarr configuration
        self.api_key = api_key or os.getenv('SONARR_API_KEY')
        self.base_url = base_url or os.getenv('SONARR_URL')
        
        if not self.api_key or not self.base_url:
            error_msg = "Missing required configuration: API key and/or base URL"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Initialize cache and session
        self.cache = SonarrCache(logger=self.logger)
        self.session = None
        
        self.logger.info("Sonarr client initialized successfully")
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(headers={
            'X-Api-Key': self.api_key,
            'Accept': 'application/json'
        })
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    async def _make_request(self, endpoint: str, method: str = 'GET', params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make an async request to the Sonarr API"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers={
                'X-Api-Key': self.api_key,
                'Accept': 'application/json'
            })
            
        url = urljoin(self.base_url, f'/api/v3/{endpoint}')
        
        try:
            async with self.session.request(method, url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            error_msg = f"Error making request to Sonarr API: {str(e)}"
            self.logger.error(error_msg)
            raise SonarrError(error_msg) from e

    async def get_series(self) -> List[Dict[str, Any]]:
        """Get all series from Sonarr"""
        if self.cache.needs_update():
            self.logger.debug("Cache needs update, fetching all series")
            shows = await self._make_request('series')
            shows_data = {show['id']: show for show in shows}
            self.cache.bulk_update_shows(shows_data)
            return shows
        return list(self.cache.shows.values())

    async def get_series_by_id(self, series_id: int) -> Dict[str, Any]:
        """Get a specific series by ID"""
        # Check cache first
        cached_show = self.cache.get_show(series_id)
        if cached_show:
            return cached_show
            
        # Cache miss - update entire cache if needed
        if self.cache.needs_update():
            self.logger.debug("Cache expired, refreshing all shows")
            shows = await self._make_request('series')
            shows_data = {show['id']: show for show in shows}
            self.cache.bulk_update_shows(shows_data)
            
            # Check if our show is in the updated cache
            cached_show = self.cache.get_show(series_id)
            if cached_show:
                return cached_show
        
        # Still not found - fetch individual show
        self.logger.debug(f"Cache miss for series {series_id}, fetching from API")
        try:
            show = await self._make_request(f'series/{series_id}')
            self.cache.update_show(show)
            return show
        except SonarrError as e:
            if "404" in str(e):  # Show not found
                self.logger.warning(f"Series {series_id} not found in Sonarr")
                return None
            raise

    async def get_episodes_by_series_id(self, series_id: int) -> List[Dict[str, Any]]:
        """Get all episodes for a specific series"""
        self.logger.debug(f"Fetching episodes for series ID: {series_id}")
        return await self._make_request(f'episode?seriesId={series_id}')

    async def get_season_by_series_id(self, series_id: int, season_number: int) -> List[Dict[str, Any]]:
        """Get all episodes for a specific season of a series"""
        cached_season = self.cache.get_season(series_id, season_number)
        if cached_season:
            return cached_season['episodes']
            
        self.logger.debug(f"Cache miss for season {season_number} of series {series_id}, fetching from API")
        episodes = await self.get_episodes_by_series_id(series_id)
        season_episodes = [ep for ep in episodes if ep.get('seasonNumber') == season_number]
        
        # Cache the season and its episodes
        season_data = {
            'seriesId': series_id,
            'seasonNumber': season_number,
            'episodeCount': len(season_episodes),
            'episodes': season_episodes
        }
        self.cache.update_season(series_id, season_number, season_data)
        
        for ep in season_episodes:
            ep_num = ep.get('episodeNumber')
            if ep_num is not None:
                self.cache.update_episode(series_id, season_number, ep_num, ep)
                
        return season_episodes

    async def get_calendar(self, start_date: Optional[datetime] = None, 
                         end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get calendar entries from Sonarr"""
        params = {}
        if start_date:
            params['start'] = start_date.strftime('%Y-%m-%d')
        if end_date:
            params['end'] = end_date.strftime('%Y-%m-%d')
            
        self.logger.debug(f"Fetching calendar from {start_date} to {end_date}")
        return await self._make_request('calendar', params=params)

    async def get_episodes_calendar(self, past_days: int = 7, future_days: int = 7) -> List[Dict[str, Any]]:
        """Get episodes from calendar for a range of days"""
        current_date = datetime.now()
        start_date = current_date - timedelta(days=past_days)
        end_date = current_date + timedelta(days=future_days)
        
        self.logger.debug(f"Fetching episodes from {past_days} days ago to {future_days} days ahead")
        return await self.get_calendar(start_date, end_date)

    async def handle_webhook(self, event_data: dict) -> None:
        """Handle incoming webhook events from Sonarr"""
        try:
            event_type = event_data.get('eventType')
            if not event_type:
                self.logger.error("Received webhook with no eventType")
                return
            
            self.logger.info(f"Received Sonarr webhook event: {event_type}")
            self.logger.debug(f"Event data: {event_data}")
            
            # Handle different event types
            if event_type == "Download":
                await self._handle_download_event(event_data)
            elif event_type == "Grab":
                await self._handle_grab_event(event_data)
            elif event_type == "Rename":
                await self._handle_rename_event(event_data)
            else:
                self.logger.warning(f"Unhandled event type: {event_type}")
        except Exception as e:
            self.logger.error(f"Error handling webhook: {str(e)}")
            raise

    async def _handle_download_event(self, event_data: dict) -> None:
        """Handle download completed events"""
        try:
            series = event_data.get('series', {})
            episode = event_data.get('episodes', [{}])[0]
            
            # Update cache with new episode data
            series_id = series.get('id')
            if series_id:
                self.cache.update_show(series)
                
                season_num = episode.get('seasonNumber')
                ep_num = episode.get('episodeNumber')
                if season_num is not None and ep_num is not None:
                    self.cache.update_episode(series_id, season_num, ep_num, episode)
                    
            self.logger.info(f"Download completed: {series.get('title')} - S{episode.get('seasonNumber', 0)}E{episode.get('episodeNumber', 0)}")
        except Exception as e:
            self.logger.error(f"Error handling download event: {str(e)}")
            raise

    async def _handle_grab_event(self, event_data: dict) -> None:
        """Handle episode grab events"""
        try:
            series = event_data.get('series', {})
            episode = event_data.get('episodes', [{}])[0]
            self.logger.info(f"Episode grabbed: {series.get('title')} - S{episode.get('seasonNumber', 0)}E{episode.get('episodeNumber', 0)}")
        except Exception as e:
            self.logger.error(f"Error handling grab event: {str(e)}")
            raise

    async def _handle_rename_event(self, event_data: dict) -> None:
        """Handle rename events"""
        try:
            series = event_data.get('series', {})
            self.logger.info(f"Series renamed: {series.get('title')}")
            
            # Update cache with renamed series
            series_id = series.get('id')
            if series_id:
                self.cache.update_show(series)
        except Exception as e:
            self.logger.error(f"Error handling rename event: {str(e)}")
            raise

    async def initialize_cache(self) -> None:
        """Initialize the cache with all shows and their seasons"""
        try:
            # Get all shows
            shows = await self.get_series()
            shows_data = {show['id']: show for show in shows}
            self.cache.bulk_update_shows(shows_data)
            
            # Get all seasons and episodes for each show
            seasons_data = {}
            episodes_data = {}
            for show in shows:
                series_id = show['id']
                episodes = await self.get_episodes_by_series_id(series_id)
                
                # Group episodes by season
                season_episodes = {}
                for episode in episodes:
                    season_num = episode.get('seasonNumber')
                    if season_num is not None:
                        if season_num not in season_episodes:
                            season_episodes[season_num] = []
                        season_episodes[season_num].append(episode)
                
                # Create season entries and cache episodes
                for season_num, season_eps in season_episodes.items():
                    season_key = f"{series_id}_{season_num}"
                    seasons_data[season_key] = {
                        'seriesId': series_id,
                        'seasonNumber': season_num,
                        'episodeCount': len(season_eps),
                        'episodes': season_eps
                    }
                    
                    # Cache individual episodes
                    for ep in season_eps:
                        ep_num = ep.get('episodeNumber')
                        if ep_num is not None:
                            ep_key = f"{series_id}_{season_num}_{ep_num}"
                            episodes_data[ep_key] = ep
            
            self.cache.bulk_update_seasons(seasons_data)
            self.cache.bulk_update_episodes(episodes_data)
            
            self.logger.info("Cache initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing cache: {str(e)}")
            raise
