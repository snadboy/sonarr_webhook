import logging
import os
from dotenv import load_dotenv
import uvicorn
from sonarr import Sonarr
from notion_db import NotionDB
from api import initialize_api
from notion_db import NotionPropertyType

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
    sonarr = Sonarr(log_level=log_level, logger=logger)
    notion = NotionDB(token=os.getenv('NOTION_TOKEN'), logger=logger)

    cs = notion.get_child_databases(page_id='14c84b8f894080458002f0463ce6175b')
    cals = sonarr.get_episodes_calendar(14, 14)

    for cal in cals:
        properties = {
            "Show Title": notion.format_property(NotionPropertyType.RICH_TEXT, f"{cal.get('series', {}).get('title', '??')} - S{cal.get('seasonNumber', '??')}E{cal.get('episodeNumber', '??')}: {cal.get('title', '??')}"),
            "Name": notion.format_property(NotionPropertyType.TITLE, cal.get('series', {}).get('title', '??')),
            "Date": notion.format_property(NotionPropertyType.DATE, cal.get('airDate', '2024-12-03')),
        }
        notion.create_or_update_row(database_id=cs[3]['id'], properties=properties)

    # Initialize FastAPI app
    app = initialize_api(sonarr)
    
    return app

app = main()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
