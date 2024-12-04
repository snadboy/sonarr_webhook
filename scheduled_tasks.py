import logging
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from notion_db import NotionDB, NotionPropertyType
from sonarr import SonarrAPI
from youtube_api import YouTubeAPI


class ScheduledTasks:
    def __init__(self, notion_client: NotionDB, sonarr_client: SonarrAPI, youtube_client: YouTubeAPI, logger: logging.Logger):
        self.notion = notion_client
        self.sonarr = sonarr_client
        self.youtube = youtube_client
        self.logger = logger

    def update_databases(self):
        """Update all databases - runs at startup and midnight"""
        self.logger.info(f"Running scheduled database updates at {datetime.now()}")
        try:
            notion_telly_children = self.notion.get_child_databases(page_id=os.getenv('NOTION_PAGE_ID_TELLY'))
            cals = self.sonarr.get_episodes_calendar(14, 14)
            series_cache = {}  # Cache for series data

            # Clear and update the "Upcoming Episodes" database
            self.notion.clear_database(notion_telly_children[os.getenv('NOTION_DB_TV_CALENDAR')]['id'])
            for cal in cals:
                # Get series info from cache or API
                series_id = cal['seriesId']
                if series_id not in series_cache:
                    series_cache[series_id] = self.sonarr.get_series_by_id(series_id)
                
                series = series_cache[series_id]
                show_title = series.get('title', 'Unknown Show')
                season_number = cal.get('seasonNumber', 0)
                episode_number = cal.get('episodeNumber', 0)
                episode_title = cal.get('title', 'Unknown Episode')

                properties = {
                    "Show Title": self.notion.format_property(NotionPropertyType.RICH_TEXT, f"{show_title} - S{season_number}E{episode_number}: {episode_title}"),
                    "Name": self.notion.format_property(NotionPropertyType.TITLE, show_title),
                    "Date": self.notion.format_property(NotionPropertyType.DATE, cal.get('airDate', '2024-12-03')),
                }
                self.notion.create_or_update_row(
                    database_id=notion_telly_children[os.getenv('NOTION_DB_TV_CALENDAR')]['id'], 
                    properties=properties
                )
            self.logger.info("Database updates completed successfully")
        except Exception as e:
            self.logger.error(f"Error in scheduled database updates: {str(e)}")

    def update_youtube_stats(self):
        """Update YouTube channel stats - runs hourly"""
        self.logger.info(f"Running YouTube stats update at {datetime.now()}")
        try:
            notion_youtube_children = self.notion.get_child_databases(page_id=os.getenv('NOTION_PAGE_ID_YOUTUBE'))
            
            # Get channel stats for A Measure of Passion
            channel_id = self.youtube.get_channel_id('@ameasureofpassion')
            channel_stats = self.youtube.get_channel_stats(channel_id)
            
            # Update the Channel Stats database
            self.notion.clear_database(notion_youtube_children[os.getenv('NOTION_DB_YT_CHANNEL')]['id'])
            properties = {
                "Name": self.notion.format_property(NotionPropertyType.TITLE, "A Measure of Passion"),
                "Subscribers": self.notion.format_property(NotionPropertyType.NUMBER, channel_stats.get('subscriberCount', 0)),
                "Views": self.notion.format_property(NotionPropertyType.NUMBER, channel_stats.get('viewCount', 0)),
                "Videos": self.notion.format_property(NotionPropertyType.NUMBER, channel_stats.get('videoCount', 0)),
                "Last Updated": self.notion.format_property(NotionPropertyType.DATE, datetime.now().isoformat()),
            }
            self.notion.create_or_update_row(
                database_id=notion_youtube_children[os.getenv('NOTION_DB_YT_CHANNEL')]['id'],
                properties=properties
            )
            self.logger.info("YouTube stats update completed successfully")
        except Exception as e:
            self.logger.error(f"Error updating YouTube stats: {str(e)}")

    @staticmethod
    def initialize_scheduler(notion_client: NotionDB, sonarr_client: SonarrAPI, youtube_client: YouTubeAPI, logger: logging.Logger) -> BackgroundScheduler:
        """Initialize and start the APScheduler"""
        scheduler = BackgroundScheduler()
        tasks = ScheduledTasks(notion_client, sonarr_client, youtube_client, logger)
        
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
        tasks.update_databases()
        tasks.update_youtube_stats()
        return scheduler
