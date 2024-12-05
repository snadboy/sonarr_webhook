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
        self.sonarr = sonarr_client
        self.youtube = youtube_client
        self.logger = logger

    async def update_databases(self):
        """Update all databases - runs at startup and midnight"""

        self.logger.info(f"Running scheduled database updates at {datetime.now()}")

        try:
            # Get configuration from environment
            past_days = int(os.getenv('SONARR_PAST_DAYS', '7'))
            future_days = int(os.getenv('SONARR_FUTURE_DAYS', '14'))

            # Get database ID
            calendar_db = await self.notion.notion_db_tv_calendar
            
            # Get calendar episodes
            cals = await self.sonarr.get_episodes_calendar(past_days, future_days)

            # Delete old entries
            filter_params = {
                "property": "Date",
                "date": {
                    "before": (datetime.now() - timedelta(days=past_days)).date().isoformat()
                }
            }
            await self.notion.delete_pages_where(calendar_db['id'], filter_params)
            
            # Add new entries
            for cal in cals:
                series_id = cal.get('seriesId', 0)
                show = self.sonarr.cache.get_show(series_id)
                if not show:
                    self.logger.warning(f"Could not find series {series_id} in cache for calendar entry")
                    continue

                show_title = show.get('title', 'Unknown Show')
                season_number = cal.get('seasonNumber', 0)
                episode_number = cal.get('episodeNumber', 0)
                episode_title = cal.get('title', 'Unknown Episode')
                episode_id = cal.get('id', 0)
                air_date = cal.get('airDate', '2024-12-03')

                # Get poster URL from show images
                poster_url = show.get('images', [{'remoteUrl': ''}])[0].get('remoteUrl', '')
                
                properties = {
                    "Show Title": self.notion.format_property(NotionPropertyType.RICH_TEXT, f"S{season_number}E{episode_number}: {episode_title}"),
                    "Name": self.notion.format_property(NotionPropertyType.TITLE, show_title),
                    "Date": self.notion.format_property(NotionPropertyType.DATE, air_date),
                    "Episode ID": self.notion.format_property(NotionPropertyType.NUMBER, episode_id),
                    "Poster": self.notion.format_property(NotionPropertyType.FILES, {
                        "url": poster_url,
                        "name": f"{show_title} Poster"
                    })
                }

                # Filter by Episode ID and Date to find existing entry
                filter_params = {
                    "and": [
                        {
                            "property": "Episode ID",
                            "number": {
                                "equals": episode_id
                            }
                        },
                        {
                            "property": "Date",
                            "date": {
                                "equals": air_date
                            }
                        }
                    ]
                }

                self.logger.info(f"Creating/Updating calendar entry for {show_title} - S{season_number}E{episode_number} on {air_date}")
                await self.notion.create_or_update_row(
                    database_id=calendar_db['id'],
                    properties=properties,
                    filter_params=filter_params
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

    async def update_youtube_channels(self):
        """Update YouTube channel database - runs at startup and daily"""
        self.logger.info(f"Running scheduled YouTube channel updates at {datetime.now()}")

        try:
            # Get database ID
            youtube_db = await self.notion.notion_db_yt_channel
            
            # Get existing channels from database
            existing_channels = await self.notion.query_database(youtube_db['id'])
            
            # Update each channel
            for channel in existing_channels:
                try:
                    channel_id = channel['properties'].get('Channel ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
                    if not channel_id:
                        self.logger.warning(f"No channel ID found for row {channel['id']}")
                        continue
                        
                    channel_info = await self.youtube.get_channel_info(channel_id)
                    if not channel_info:
                        self.logger.warning(f"Could not get info for channel {channel_id}")
                        continue
                        
                    properties = {
                        "Name": self.notion.format_property(NotionPropertyType.TITLE, channel_info['title']),
                        "Description": self.notion.format_property(NotionPropertyType.RICH_TEXT, channel_info['description']),
                        "Channel ID": self.notion.format_property(NotionPropertyType.RICH_TEXT, channel_id),
                        "Subscriber Count": self.notion.format_property(NotionPropertyType.NUMBER, channel_info['subscriberCount']),
                        "Video Count": self.notion.format_property(NotionPropertyType.NUMBER, channel_info['videoCount']),
                        "View Count": self.notion.format_property(NotionPropertyType.NUMBER, channel_info['viewCount']),
                        "Last Updated": self.notion.format_property(NotionPropertyType.DATE, datetime.now().isoformat()),
                    }
                    
                    self.logger.info(f"Updating channel {channel_info['title']}")
                    await self.notion.update_page(page_id=channel['id'], properties=properties)
                    
                except Exception as e:
                    self.logger.error(f"Error updating channel {channel.get('id', 'Unknown')}: {str(e)}")
                    continue
                    
            self.logger.info("YouTube channel updates completed successfully")
        except Exception as e:
            self.logger.error(f"Error in scheduled YouTube channel updates: {str(e)}")

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
        
        # Add job to run daily for YouTube channels
        scheduler.add_job(
            tasks.update_youtube_channels,
            trigger=CronTrigger(hour=0, minute=0),  # Run at midnight every day
            id='daily_youtube_channels_update',
            name='Daily YouTube channels update',
            replace_existing=True
        )
        
        # Start the scheduler
        scheduler.start()
        logger.info("Scheduler started")
        
        # Run initial updates
        asyncio.create_task(tasks.update_databases())
        asyncio.create_task(tasks.update_youtube_stats())
        asyncio.create_task(tasks.update_youtube_channels())
        
        return scheduler

    @staticmethod
    def register_startup_handler(app: FastAPI, notion_client: NotionDB, sonarr_client: Sonarr, youtube_client: YouTubeAPI, logger: logging.Logger):
        """Register the startup event handler with FastAPI"""
        @app.on_event("startup")
        async def startup_event():
            await ScheduledTasks.initialize_scheduler(notion_client, sonarr_client, youtube_client, logger)
