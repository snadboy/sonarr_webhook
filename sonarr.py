import logging
import os
from typing import Optional
import requests
from dotenv import load_dotenv

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
            self.logger.error(f"Error processing webhook: {e}", exc_info=True)
    
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
