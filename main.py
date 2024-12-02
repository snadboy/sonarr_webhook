import logging
import os
from dotenv import load_dotenv
import uvicorn
from sonarr import Sonarr
from notion_db import NotionDB
from api import initialize_api
from notion_db import NotionPropertyType
from youtube_api import YouTubeAPI

def main():
    # Load environment variables
    load_dotenv()

    # Setup logging
    log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))
    logger = logging.getLogger()
    if not logger.handlers:  # Only add handler if no handlers exist
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)

    # Initialize clients
    sonarr = Sonarr(api_key=os.getenv('SONARR_API_KEY'), base_url=os.getenv('SONARR_URL'), log_level=log_level, logger=logger)
    notion = NotionDB(token=os.getenv('NOTION_TOKEN'), logger=logger, log_level=log_level)
    youtube = YouTubeAPI(api_key=os.getenv('YOUTUBE_API_KEY'), log_level=log_level, logger=logger)

    # Get channel ID from handle
    try:
        channel_id = youtube.get_channel_id('@ameasureofpassion')
        logger.info(f"Found channel ID: {channel_id}")
    except Exception as e:
        logger.error(f"Failed to get channel ID: {str(e)}")
        raise
    try:
        channel_stats = youtube.get_channel_stats(channel_id)
        logger.info(f"Channel stats: {channel_stats}")
        channel_videos = youtube.get_channel_videos(channel_id)
        logger.info(f"Channel videos: {channel_videos}")
    except Exception as e:
        logger.error(f"Failed to get channel stats: {str(e)}")
        raise

    cs = notion.get_child_databases(page_id='14c84b8f894080458002f0463ce6175b')
    cals = sonarr.get_episodes_calendar(14, 14)
    series_cache = {}  # Cache for series data

    notion.clear_database(cs[3]['id'])
    for cal in cals:
        # Get series info from cache or API
        series_id = cal['seriesId']
        if series_id not in series_cache:
            series_cache[series_id] = sonarr.get_series_by_id(series_id)
        
        series = series_cache[series_id]
        show_title = series.get('title', 'Unknown Show')
        season_number = cal.get('seasonNumber', 0)
        episode_number = cal.get('episodeNumber', 0)
        episode_title = cal.get('title', 'Unknown Episode')

        properties = {
            "Show Title": notion.format_property(NotionPropertyType.RICH_TEXT, f"{show_title} - S{season_number}E{episode_number}: {episode_title}"),
            "Name": notion.format_property(NotionPropertyType.TITLE, show_title),
            "Date": notion.format_property(NotionPropertyType.DATE, cal.get('airDate', '2024-12-03')),
        }
        notion.create_or_update_row(database_id=cs[3]['id'], properties=properties)

    # Initialize FastAPI app
    app = initialize_api(sonarr)
    
    return app

app = main()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
