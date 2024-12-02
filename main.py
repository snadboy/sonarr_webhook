import logging
import os
from dotenv import load_dotenv
import uvicorn
from sonarr import Sonarr
from api import initialize_api

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

    # Initialize Sonarr client
    sonarr = Sonarr(log_level=log_level, logger=logger)
    
    # Initialize FastAPI app
    app = initialize_api(sonarr)
    
    return app

app = main()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
