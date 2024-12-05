import logging
import os
import asyncio
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from notion_db import NotionDB, NotionPropertyType, NotionDBError
from sonarr import Sonarr
from youtube_api import YouTubeAPI
from fastapi import FastAPI
from typing import Dict, Any


class ScheduledTasks:
    def __init__(self, notion_client: NotionDB, sonarr_client: Sonarr, youtube_client: YouTubeAPI, logger: logging.Logger):
        self.notion = notion_client
        self._notion_telly = None
        self._notion_youtube = None
        self.sonarr = sonarr_client
        self.youtube = youtube_client
        self.logger = logger

    async def get_notion_page_info(self, page_name: str) -> Dict[str, Any]:
        """Get Notion page info including its databases.
        
        Args:
            page_name (str): Name of the page as configured in environment variables
            
        Returns:
            Dict containing page info with 'name', 'page_id', and 'database_ids'
            
        Raises:
            NotionDBError: If page cannot be found
        """
        page_info = {"name": page_name}
        pages = await self.notion.get_pages()
        page_data = pages.get(page_name)
        if not page_data:
            raise NotionDBError(f"Could not find page {page_name}")
        page_info["page_id"] = page_data['id']
        page_info["database_ids"] = await self.notion.get_child_databases(page_id=page_info["page_id"])
        return page_info

    @property
    async def notion_telly(self) -> Dict[str, Any]:
        """Cached access to Telly page info.
        
        Returns:
            Dict containing page info with 'name', 'page_id', and 'database_ids'
        """
        if self._notion_telly is None:
            self._notion_telly = await self.get_notion_page_info(os.getenv('NOTION_PAGE_TELLY'))
        return self._notion_telly

    @property
    async def notion_youtube(self) -> Dict[str, Any]:
        """Cached access to YouTube page info.
        
        Returns:
            Dict containing page info with 'name', 'page_id', and 'database_ids'
        """
        if self._notion_youtube is None:
            self._notion_youtube = await self.get_notion_page_info(os.getenv('NOTION_PAGE_YOUTUBE'))
        return self._notion_youtube

    async def update_databases(self):
        """Update all databases - runs at startup and midnight"""

        self.logger.info(f"Running scheduled database updates at {datetime.now()}")

        try:
            # Get configuration from environment
            past_days = int(os.getenv('SONARR_PAST_DAYS', '7'))
            future_days = int(os.getenv('SONARR_FUTURE_DAYS', '14'))

            # Get database IDs
            telly = await self.notion_telly
            calendar_db_id = telly["database_ids"][os.getenv('NOTION_DB_TV_CALENDAR')]['id']
            
            # Get calendar episodes
            cals = await self.sonarr.get_episodes_calendar(past_days, future_days)

            # Delete old entries
            filter_params = {
                "property": "Date",
                "date": {
                    "before": (datetime.now() - timedelta(days=past_days)).date().isoformat()
                }
            }
            await self.notion.delete_pages_where(calendar_db_id, filter_params)
            
            # Add new entries
            for cal in cals:
                series_id = cal['seriesId']
                series = await self.sonarr.get_series_by_id(series_id)
                if not series:
                    self.logger.warning(f"Could not find series {series_id} for calendar entry")
                    continue
                    
                show_title = series.get('title', 'Unknown Show')
                season_number = cal.get('seasonNumber', 0)
                episode_number = cal.get('episodeNumber', 0)
                episode_title = cal.get('title', 'Unknown Episode')

                properties = {
                    "Show Title": self.notion.format_property(NotionPropertyType.RICH_TEXT, f"{show_title} - S{season_number}E{episode_number}: {episode_title}"),
                    "Name": self.notion.format_property(NotionPropertyType.TITLE, show_title),
                    "Date": self.notion.format_property(NotionPropertyType.DATE, cal.get('airDate', '2024-12-03')),
                }
                await self.notion.create_or_update_row(
                    database_id=calendar_db_id,
                    properties=properties
                )
            self.logger.info("Database updates completed successfully")
        except Exception as e:
            self.logger.error(f"Error in scheduled database updates: {str(e)}")

    async def update_youtube_stats(self) -> None:
        """Update YouTube channel stats in Notion database"""
        try:
            self.logger.info("Starting YouTube channel stats update")
            
            # Initialize YouTube API client
            async with YouTubeAPI(log_level=self.logger.level) as youtube:
                # Get channel stats for @ameasureofpassion
                channel_id = await youtube.get_channel_id('@ameasureofpassion')
                stats = await youtube.get_channel_stats(channel_id)
                
                # Update Notion database
                await self.notion.update_youtube_channel_stats(stats)
                
            self.logger.info("YouTube channel stats update completed successfully")
        except Exception as e:
            self.logger.error(f"Error updating YouTube stats: {str(e)}")

    @staticmethod
    async def initialize_scheduler(notion_client: NotionDB, sonarr_client: Sonarr, youtube_client: YouTubeAPI, logger: logging.Logger) -> AsyncIOScheduler:
        """Initialize and start the APScheduler"""
        scheduler = AsyncIOScheduler()
        tasks = ScheduledTasks(notion_client, sonarr_client, youtube_client, logger)
        
        # Initialize Sonarr cache
        logger.info("Initializing Sonarr cache...")
        await sonarr_client.initialize_cache()
        
        # Add job to run at midnight every day
        scheduler.add_job(
            tasks.update_databases,
            trigger=CronTrigger(hour=0, minute=0),
            id='midnight_update',
            name='Midnight database update',
            replace_existing=True
        )
        
        # Add job to run hourly for YouTube stats
        scheduler.add_job(
            tasks.update_youtube_stats,
            trigger=CronTrigger(minute=0),  # Run at the start of every hour
            id='hourly_youtube_update',
            name='Hourly YouTube stats update',
            replace_existing=True
        )
        
        # Start the scheduler
        scheduler.start()
        logger.info("Scheduler started")
        
        # Run initial updates
        asyncio.create_task(tasks.update_databases())
        asyncio.create_task(tasks.update_youtube_stats())
        
        return scheduler

    @staticmethod
    def register_startup_handler(app: FastAPI, notion_client: NotionDB, sonarr_client: Sonarr, youtube_client: YouTubeAPI, logger: logging.Logger):
        """Register the startup event handler with FastAPI"""
        @app.on_event("startup")
        async def startup_event():
            await ScheduledTasks.initialize_scheduler(notion_client, sonarr_client, youtube_client, logger)
