import logging
from logging.handlers import RotatingFileHandler
import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Depends
from api import initialize_api
from notion_db import NotionDB, NotionPropertyType
from sonarr import Sonarr
from youtube_api import YouTubeAPI
from scheduled_tasks import ScheduledTasks

# Load environment variables
load_dotenv()

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create console handler with INFO level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Create file handler with DEBUG level and rotation
file_handler = RotatingFileHandler(
    os.path.join(log_dir, 'app.log'),
    maxBytes=5000 * 1024,  # 5000 lines approximately (assuming average line length of 1KB)
    backupCount=3  # Keep 3 backup files (4 files total)
)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Set third-party loggers to INFO level to reduce noise
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('httpx').setLevel(logging.INFO)
logging.getLogger('httpcore').setLevel(logging.INFO)
logging.getLogger('apscheduler').setLevel(logging.INFO)

# Initialize clients
sonarr = Sonarr(api_key=os.getenv('SONARR_API_KEY'), base_url=os.getenv('SONARR_URL'), log_level=logging.DEBUG, logger=logger)
notion = NotionDB(token=os.getenv('NOTION_TOKEN'), logger=logger, log_level=logging.DEBUG)
youtube = YouTubeAPI(api_key=os.getenv('YOUTUBE_API_KEY'), log_level=logging.DEBUG, logger=logger)

# Initialize FastAPI app
app = initialize_api(sonarr)

# Register startup handler
ScheduledTasks.register_startup_handler(app, notion, sonarr, youtube, logger)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
