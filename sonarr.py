import logging
import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from urllib.parse import urljoin

class SonarrError(Exception):
    """Base exception for Sonarr API errors"""
    pass

class Sonarr:
    def __init__(self, log_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        """
        Initialize Sonarr client
        
        Args:
            log_level (int): Logging level (default: logging.INFO)
            logger (Optional[logging.Logger]): Custom logger instance (default: None)
        """
        # Load environment variables
        load_dotenv()
        
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
        
        # Initialize Sonarr configuration
        self.api_key = os.getenv('SONARR_API_KEY')
        self.base_url = os.getenv('SONARR_URL')
        
        if not self.api_key or not self.base_url:
            error_msg = "Missing required environment variables: SONARR_API_KEY and/or SONARR_URL"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info("Sonarr client initialized successfully")

    def _make_request(self, endpoint: str, method: str = 'GET', params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to the Sonarr API
        
        Args:
            endpoint (str): API endpoint
            method (str): HTTP method (default: 'GET')
            params (Optional[Dict[str, Any]]): Query parameters
            
        Returns:
            Dict[str, Any]: Response data
            
        Raises:
            SonarrError: If the API request fails
        """
        url = urljoin(self.base_url, f'/api/v3/{endpoint}')
        headers = {
            'X-Api-Key': self.api_key,
            'Accept': 'application/json'
        }
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"Error making request to Sonarr API: {str(e)}"
            self.logger.error(error_msg)
            raise SonarrError(error_msg) from e

    def get_series(self) -> List[Dict[str, Any]]:
        """
        Get all series from Sonarr
        
        Returns:
            List[Dict[str, Any]]: List of series
        """
        self.logger.debug("Fetching all series")
        return self._make_request('series')

    def get_series_by_id(self, series_id: int) -> Dict[str, Any]:
        """
        Get a specific series by ID
        
        Args:
            series_id (int): Series ID
            
        Returns:
            Dict[str, Any]: Series data
        """
        self.logger.debug(f"Fetching series with ID: {series_id}")
        return self._make_request(f'series/{series_id}')

    def get_episodes_by_series_id(self, series_id: int) -> List[Dict[str, Any]]:
        """
        Get all episodes for a specific series
        
        Args:
            series_id (int): Series ID
            
        Returns:
            List[Dict[str, Any]]: List of episodes
        """
        self.logger.debug(f"Fetching episodes for series ID: {series_id}")
        return self._make_request(f'episode?seriesId={series_id}')

    def get_calendar(self, start_date: Optional[datetime] = None, 
                    end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get calendar entries from Sonarr
        
        Args:
            start_date (Optional[datetime]): Start date (default: today)
            end_date (Optional[datetime]): End date (default: 7 days from start)
            
        Returns:
            List[Dict[str, Any]]: List of calendar entries
        """
        # Set default dates if not provided
        if start_date is None:
            start_date = datetime.now()
        if end_date is None:
            end_date = start_date + timedelta(days=7)

        # Format dates for API
        params = {
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d')
        }
        
        self.logger.debug(f"Fetching calendar from {params['start']} to {params['end']}")
        return self._make_request('calendar', params=params)

    def get_episodes_calendar(self, past_days: int = 7, future_days: int = 7) -> List[Dict[str, Any]]:
        """
        Get episodes from calendar for a range of days before and after today
        
        Args:
            past_days (int): Number of days to look back (default: 7)
            future_days (int): Number of days to look ahead (default: 7)
            
        Returns:
            List[Dict[str, Any]]: List of episodes in the date range
        """
        current_date = datetime.now()
        start_date = current_date - timedelta(days=past_days)
        end_date = current_date + timedelta(days=future_days)
        
        self.logger.debug(f"Fetching episodes from {past_days} days ago to {future_days} days ahead")
        return self.get_calendar(start_date, end_date)

    def get_season_by_series_id(self, series_id: int, season_number: int) -> List[Dict[str, Any]]:
        """
        Get all episodes for a specific season of a series
        
        Args:
            series_id (int): Series ID
            season_number (int): Season number
            
        Returns:
            List[Dict[str, Any]]: List of episodes in the season
        """
        self.logger.debug(f"Fetching season {season_number} for series ID: {series_id}")
        episodes = self.get_episodes_by_series_id(series_id)
        return [ep for ep in episodes if ep.get('seasonNumber') == season_number]

    def handle_webhook(self, event_data: dict) -> None:
        """
        Handle incoming webhook events from Sonarr
        
        Args:
            event_data (dict): The webhook payload from Sonarr
        """
        try:
            event_type = event_data.get('eventType')
            if not event_type:
                self.logger.error("Received webhook with no eventType")
                return
            
            self.logger.info(f"Received Sonarr webhook event: {event_type}")
            self.logger.debug(f"Event data: {event_data}")
            
            # Handle different event types
            if event_type == "Download":
                self._handle_download_event(event_data)
            elif event_type == "Grab":
                self._handle_grab_event(event_data)
            elif event_type == "Rename":
                self._handle_rename_event(event_data)
            else:
                self.logger.warning(f"Unhandled event type: {event_type}")
        except Exception as e:
            self.logger.error(f"Error handling webhook event: {str(e)}")
            raise

    def _handle_download_event(self, event_data: dict) -> None:
        """Handle download completed events"""
        series_title = event_data.get('series', {}).get('title')
        episode_title = event_data.get('episodes', [{}])[0].get('title')
        self.logger.info(f"Download completed: {series_title} - {episode_title}")
    
    def _handle_grab_event(self, event_data: dict) -> None:
        """Handle episode grab events"""
        series_title = event_data.get('series', {}).get('title')
        episode_title = event_data.get('episodes', [{}])[0].get('title')
        self.logger.info(f"Episode grabbed: {series_title} - {episode_title}")
    
    def _handle_rename_event(self, event_data: dict) -> None:
        """Handle rename events"""
        series_title = event_data.get('series', {}).get('title')
        self.logger.info(f"Rename event for series: {series_title}")
