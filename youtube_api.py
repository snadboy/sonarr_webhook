import logging
import json
from typing import Optional, Dict, Any, List
import os
import aiohttp
from urllib.parse import urlparse, parse_qs

class YouTubeAPIError(Exception):
    """Base exception for YouTube API errors"""
    pass

class YouTubeAPI:
    BASE_URL = "https://www.googleapis.com/youtube/v3"
    
    def __init__(self, api_key: Optional[str] = None, log_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        """
        Initialize YouTube API client
        
        Args:
            api_key (Optional[str]): YouTube Data API key (default: from env YOUTUBE_API_KEY)
            log_level (int): Logging level (default: logging.INFO)
            logger (Optional[logging.Logger]): Custom logger instance
        """
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                handler.setLevel(log_level)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        
        self.logger.setLevel(log_level)
        
        self.api_key = api_key or os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            error_msg = "Missing YouTube API key"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.session = None
        self.logger.info("YouTube API client initialized successfully")

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make an HTTP request to the YouTube Data API"""
        if not self.session:
            self.session = aiohttp.ClientSession()

        params['key'] = self.api_key
        url = f"{self.BASE_URL}/{endpoint}"

        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    async def get_video_stats(self, video_id: str) -> Dict[str, Any]:
        """
        Get statistics for a specific video
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            Dict[str, Any]: Video statistics including viewCount, likeCount, commentCount
            
        Raises:
            YouTubeAPIError: If API request fails
        """
        try:
            response = await self._make_request('videos', {
                'part': 'statistics,snippet',
                'id': video_id
            })
            
            if not response['items']:
                raise YouTubeAPIError(f"Video not found: {video_id}")
                
            video = response['items'][0]
            return {
                'title': video['snippet']['title'],
                'views': int(video['statistics'].get('viewCount', 0)),
                'likes': int(video['statistics'].get('likeCount', 0)),
                'comments': int(video['statistics'].get('commentCount', 0)),
                'published_at': video['snippet']['publishedAt']
            }
        except Exception as e:
            error_msg = f"Error getting video stats: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    async def get_channel_stats(self, channel_id: str) -> Dict[str, Any]:
        """
        Get statistics for a YouTube channel
        
        Args:
            channel_id (str): YouTube channel ID
            
        Returns:
            Dict[str, Any]: Channel statistics including subscriber count, video count, view count
            
        Raises:
            YouTubeAPIError: If API request fails
        """
        try:
            response = await self._make_request('channels', {
                'part': 'statistics,snippet',
                'id': channel_id
            })
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_id}")
                
            channel = response['items'][0]
            return {
                'title': channel['snippet']['title'],
                'subscriberCount': int(channel['statistics'].get('subscriberCount', 0)),
                'viewCount': int(channel['statistics'].get('viewCount', 0)),
                'videoCount': int(channel['statistics'].get('videoCount', 0))
            }
        except Exception as e:
            error_msg = f"Error getting channel stats: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    async def get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Get comments for a specific video
        
        Args:
            video_id (str): YouTube video ID
            max_results (int): Maximum number of comments to retrieve (default: 100)
            
        Returns:
            List[Dict[str, Any]]: List of comments with author, text, and metadata
            
        Raises:
            YouTubeAPIError: If API request fails
        """
        try:
            response = await self._make_request('commentThreads', {
                'part': 'snippet',
                'videoId': video_id,
                'maxResults': min(max_results, 100),
                'order': 'relevance'
            })
            
            comments = []
            for item in response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'likes': int(comment.get('likeCount', 0)),
                    'published_at': comment['publishedAt']
                })
            return comments
        except Exception as e:
            error_msg = f"Error getting video comments: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    async def get_channel_videos(self, channel_id: str, max_results: int = 50) -> List[Dict[str, Any]]:
        """
        Get videos from a specific channel
        
        Args:
            channel_id (str): YouTube channel ID
            max_results (int): Maximum number of videos to retrieve (default: 50)
            
        Returns:
            List[Dict[str, Any]]: List of videos with title, ID, and metadata
            
        Raises:
            YouTubeAPIError: If API request fails
        """
        try:
            # First get the channel's upload playlist
            response = await self._make_request('channels', {
                'part': 'contentDetails',
                'id': channel_id
            })
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_id}")
                
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Then get the videos from the playlist
            response = await self._make_request('playlistItems', {
                'part': 'snippet',
                'playlistId': uploads_playlist_id,
                'maxResults': min(max_results, 50)
            })
            
            videos = []
            for item in response.get('items', []):
                snippet = item['snippet']
                videos.append({
                    'title': snippet['title'],
                    'description': snippet['description'],
                    'video_id': snippet['resourceId']['videoId'],
                    'published_at': snippet['publishedAt'],
                    'thumbnail_url': snippet['thumbnails']['default']['url']
                })
            return videos
        except Exception as e:
            error_msg = f"Error getting channel videos: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    async def get_channel_id(self, channel_url_or_username: str) -> str:
        """
        Get channel ID from a channel URL or username
        
        Args:
            channel_url_or_username (str): Can be any of:
                - Full channel URL (https://www.youtube.com/channel/UC...)
                - Custom URL (https://www.youtube.com/c/ChannelName)
                - Handle (https://www.youtube.com/@HandleName)
                - Username
                
        Returns:
            str: Channel ID
            
        Raises:
            YouTubeAPIError: If channel not found or API request fails
        """
        try:
            # If it's a URL, parse it
            if channel_url_or_username.startswith(('http://', 'https://')):
                parsed_url = urlparse(channel_url_or_username)
                path_parts = parsed_url.path.strip('/').split('/')
                
                # Direct channel ID
                if len(path_parts) >= 2 and path_parts[0] == 'channel':
                    return path_parts[1]
                
                # Handle (@username)
                if len(path_parts) >= 1 and path_parts[0].startswith('@'):
                    channel_url_or_username = path_parts[0]
            
            # Search for the channel
            response = await self._make_request('search', {
                'part': 'snippet',
                'q': channel_url_or_username,
                'type': 'channel',
                'maxResults': 1
            })
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_url_or_username}")
                
            return response['items'][0]['snippet']['channelId']
        except Exception as e:
            error_msg = f"Error getting channel ID: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e
