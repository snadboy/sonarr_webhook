import logging
from fastapi import FastAPI, Request
from sonarr import Sonarr

# Initialize FastAPI app
app = FastAPI(
    title="Sonarr Webhook API",
    description="API for handling Sonarr webhooks and retrieving series information",
    version="1.0.0"
)

def initialize_api(sonarr_client: Sonarr) -> FastAPI:
    """
    Initialize the FastAPI application with routes
    
    Args:
        sonarr_client (Sonarr): Initialized Sonarr client instance
        
    Returns:
        FastAPI: Configured FastAPI application
    """
    
    @app.post("/webhook")
    async def webhook(request: Request):
        """
        Webhook endpoint for Sonarr events
        """
        try:
            event_data = await request.json()
            sonarr_client.handle_webhook(event_data)
            return {"status": "success"}
        except Exception as e:
            logging.error(f"Error processing webhook: {str(e)}")
            return {"status": "error", "message": str(e)}

    @app.get("/health")
    async def health_check():
        """
        Health check endpoint
        """
        return {"status": "healthy"}
    
    @app.get("/calendar")
    async def get_calendar(past_days: int = 7, future_days: int = 7):
        """
        Get calendar entries for a date range
        
        Args:
            past_days (int): Number of days to look back (default: 7)
            future_days (int): Number of days to look ahead (default: 7)
        """
        try:
            calendar = sonarr_client.get_episodes_calendar(past_days=past_days, future_days=future_days)
            return {"status": "success", "data": calendar}
        except Exception as e:
            logging.error(f"Error fetching calendar: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series")
    async def get_series():
        """
        Get all series from Sonarr
        """
        try:
            series = sonarr_client.get_series()
            return {"status": "success", "data": series}
        except Exception as e:
            logging.error(f"Error fetching series: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series/{series_id}")
    async def get_series_by_id(series_id: int):
        """
        Get a specific series by ID
        """
        try:
            series = sonarr_client.get_series_by_id(series_id)
            return {"status": "success", "data": series}
        except Exception as e:
            logging.error(f"Error fetching series {series_id}: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series/{series_id}/episodes")
    async def get_episodes(series_id: int, season_number: int = None):
        """
        Get episodes for a series, optionally filtered by season
        """
        try:
            if season_number is not None:
                episodes = sonarr_client.get_season_by_series_id(series_id, season_number)
            else:
                episodes = sonarr_client.get_episodes_by_series_id(series_id)
            return {"status": "success", "data": episodes}
        except Exception as e:
            logging.error(f"Error fetching episodes for series {series_id}: {str(e)}")
            return {"status": "error", "message": str(e)}

    return app
