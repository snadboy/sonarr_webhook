import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

class SonarrCache:
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the Sonarr cache"""
        self.logger = logger or logging.getLogger(__name__)
        self.shows: Dict[int, Dict[str, Any]] = {}  # series_id -> show data
        self.seasons: Dict[str, Dict[str, Any]] = {}  # "{series_id}_{season_number}" -> season data
        self.episodes: Dict[str, Dict[str, Any]] = {}  # "{series_id}_{season_number}_{episode_number}" -> episode data
        self.last_full_update = None
        self.update_interval = timedelta(hours=12)  # Full refresh every 12 hours
        
    def needs_update(self) -> bool:
        """Check if cache needs a full update"""
        if not self.last_full_update:
            return True
        return datetime.now() - self.last_full_update > self.update_interval
    
    def update_show(self, show_data: Dict[str, Any]) -> None:
        """Update a single show in the cache"""
        series_id = show_data.get('id')
        if not series_id:
            self.logger.warning("Attempted to cache show without ID")
            return
            
        self.shows[series_id] = show_data
        self.logger.debug(f"Updated show cache for series {series_id}")
        
    def update_season(self, series_id: int, season_number: int, season_data: Dict[str, Any]) -> None:
        """Update a single season in the cache"""
        cache_key = f"{series_id}_{season_number}"
        self.seasons[cache_key] = season_data
        self.logger.debug(f"Updated season cache for {cache_key}")
        
    def update_episode(self, series_id: int, season_number: int, episode_number: int, episode_data: Dict[str, Any]) -> None:
        """Update a single episode in the cache"""
        cache_key = f"{series_id}_{season_number}_{episode_number}"
        self.episodes[cache_key] = episode_data
        self.logger.debug(f"Updated episode cache for {cache_key}")
        
    def get_show(self, series_id: int) -> Optional[Dict[str, Any]]:
        """Get show data from cache"""
        return self.shows.get(series_id)
        
    def get_season(self, series_id: int, season_number: int) -> Optional[Dict[str, Any]]:
        """Get season data from cache"""
        cache_key = f"{series_id}_{season_number}"
        return self.seasons.get(cache_key)
        
    def get_episode(self, series_id: int, season_number: int, episode_number: int) -> Optional[Dict[str, Any]]:
        """Get episode data from cache"""
        cache_key = f"{series_id}_{season_number}_{episode_number}"
        return self.episodes.get(cache_key)
    
    def bulk_update_shows(self, shows_data: Dict[int, Dict[str, Any]]) -> None:
        """Update multiple shows at once"""
        self.shows.update(shows_data)
        self.last_full_update = datetime.now()
        self.logger.info(f"Updated {len(shows_data)} shows in cache")
        
    def bulk_update_seasons(self, seasons_data: Dict[str, Dict[str, Any]]) -> None:
        """Update multiple seasons at once"""
        self.seasons.update(seasons_data)
        self.logger.info(f"Updated {len(seasons_data)} seasons in cache")
        
    def bulk_update_episodes(self, episodes_data: Dict[str, Dict[str, Any]]) -> None:
        """Update multiple episodes at once"""
        self.episodes.update(episodes_data)
        self.logger.info(f"Updated {len(episodes_data)} episodes in cache")
        
    def clear(self) -> None:
        """Clear all cache data"""
        self.shows.clear()
        self.seasons.clear()
        self.episodes.clear()
        self.last_full_update = None
        self.logger.info("Cleared all cache data")
