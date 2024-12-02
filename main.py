import logging
import os
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from sonarr import Sonarr

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

# Initialize FastAPI app
app = FastAPI()

# Initialize Sonarr client with our configured logger
sonarr = Sonarr(log_level=log_level, logger=logger)
cal = sonarr.get_episodes_calendar(past_days=7, future_days=7)

@app.post("/webhook")
async def webhook(request: Request):
    """
    Webhook endpoint for Sonarr events
    """
    try:
        event_data = await request.json()
        sonarr.handle_webhook(event_data)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
