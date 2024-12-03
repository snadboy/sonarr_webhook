import logging
import os
from typing import Optional
from fastapi import FastAPI, Request, Depends, HTTPException, status, Header
from sonarr import Sonarr

# Initialize FastAPI app
app = FastAPI(
    title="Sonarr Webhook API",
    description="API for handling Sonarr webhooks and retrieving series information",
    version="1.0.0"
)

async def verify_api_key(x_api_key: str = Header(None)) -> bool:
    """
    Verify API key from header
    
    Args:
        x_api_key (str): API key from X-API-Key header
        
    Returns:
        bool: True if API key is valid
        
    Raises:
        HTTPException: If API key is invalid or missing
    """
    correct_api_key = os.getenv('WEBHOOK_API_KEY')
    
    # If API key is not configured, allow access
    if not correct_api_key:
        return True
    
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    if x_api_key != correct_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    return True

def initialize_api(sonarr_client: Sonarr) -> FastAPI:
    """
    Initialize the FastAPI application with routes
    
    Args:
        sonarr_client (Sonarr): Initialized Sonarr client instance
        
    Returns:
        FastAPI: Configured FastAPI application
    """
    
    @app.post("/webhook")
    async def webhook(request: Request, authenticated: bool = Depends(verify_api_key)):
        """
        Webhook endpoint for Sonarr events
        
        Requires API key in X-API-Key header if WEBHOOK_API_KEY is set
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
    async def get_calendar(
        past_days: int = 7, 
        future_days: int = 7,
        authenticated: bool = Depends(verify_api_key)
    ):
        """
        Get calendar entries for a date range
        
        Args:
            past_days (int): Number of days to look back (default: 7)
            future_days (int): Number of days to look ahead (default: 7)
            
        Requires API key in X-API-Key header if WEBHOOK_API_KEY is set
        """
        try:
            return sonarr_client.get_episodes_calendar(past_days, future_days)
        except Exception as e:
            logging.error(f"Error retrieving calendar: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series")
    async def get_series(authenticated: bool = Depends(verify_api_key)):
        """
        Get all series from Sonarr
        
        Requires API key in X-API-Key header if WEBHOOK_API_KEY is set
        """
        try:
            series = sonarr_client.get_series()
            return series
        except Exception as e:
            logging.error(f"Error retrieving series: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series/{series_id}")
    async def get_series_by_id(series_id: int, authenticated: bool = Depends(verify_api_key)):
        """
        Get a specific series by ID
        
        Requires API key in X-API-Key header if WEBHOOK_API_KEY is set
        """
        try:
            series = sonarr_client.get_series_by_id(series_id)
            return series
        except Exception as e:
            logging.error(f"Error retrieving series: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @app.get("/series/{series_id}/episodes")
    async def get_episodes(series_id: int, season_number: int = None, authenticated: bool = Depends(verify_api_key)):
        """
        Get episodes for a series, optionally filtered by season
        
        Requires API key in X-API-Key header if WEBHOOK_API_KEY is set
        """
        try:
            if season_number is not None:
                episodes = sonarr_client.get_season_by_series_id(series_id, season_number)
            else:
                episodes = sonarr_client.get_episodes_by_series_id(series_id)
            return episodes
        except Exception as e:
            logging.error(f"Error retrieving episodes: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    return app
