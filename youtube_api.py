import logging
from typing import Optional, Dict, Any, List
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class YouTubeAPIError(Exception):
    """Base exception for YouTube API errors"""
    pass

class YouTubeAPI:
    def __init__(self, api_key: Optional[str] = None, log_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        """
        Initialize YouTube API client
        
        Args:
            api_key (Optional[str]): YouTube Data API key (default: from env YOUTUBE_API_KEY)
            log_level (int): Logging level (default: logging.INFO)
            logger (Optional[logging.Logger]): Custom logger instance
        """
        # Setup logging
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
        
        # Initialize YouTube API
        self.api_key = api_key or os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            error_msg = "Missing YouTube API key"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.logger.info("YouTube API client initialized successfully")
    
    def get_video_stats(self, video_id: str) -> Dict[str, Any]:
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
            request = self.youtube.videos().list(
                part="statistics,snippet",
                id=video_id
            )
            response = request.execute()
            
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
        except HttpError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e
    
    def get_channel_stats(self, channel_id: str) -> Dict[str, Any]:
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
            request = self.youtube.channels().list(
                part="statistics,snippet",
                id=channel_id
            )
            response = request.execute()
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_id}")
                
            channel = response['items'][0]
            return {
                'title': channel['snippet']['title'],
                'subscribers': int(channel['statistics'].get('subscriberCount', 0)),
                'videos': int(channel['statistics'].get('videoCount', 0)),
                'total_views': int(channel['statistics'].get('viewCount', 0)),
                'created_at': channel['snippet']['publishedAt']
            }
        except HttpError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e
    
    def get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict[str, Any]]:
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
            comments = []
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(max_results, 100),  # API limit is 100 per request
                textFormat="plainText"
            )
            
            while request and len(comments) < max_results:
                response = request.execute()
                
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comments.append({
                        'author': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'likes': int(comment.get('likeCount', 0)),
                        'published_at': comment['publishedAt'],
                        'updated_at': comment['updatedAt']
                    })
                
                # Get the next page of comments
                request = self.youtube.commentThreads().list_next(request, response)
                
                if len(comments) >= max_results:
                    break
            
            return comments[:max_results]
        except HttpError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e
    
    def get_channel_videos(self, channel_id: str, max_results: int = 50) -> List[Dict[str, Any]]:
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
            # First get the channel's upload playlist ID
            request = self.youtube.channels().list(
                part="contentDetails",
                id=channel_id
            )
            response = request.execute()
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_id}")
            
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Then get the videos from the uploads playlist
            videos = []
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=min(max_results, 50)  # API limit is 50 per request
            )
            
            while request and len(videos) < max_results:
                response = request.execute()
                
                for item in response['items']:
                    video_id = item['contentDetails']['videoId']
                    # Get additional stats for each video
                    stats = self.get_video_stats(video_id)
                    videos.append({
                        'id': video_id,
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'published_at': item['snippet']['publishedAt'],
                        'thumbnail': item['snippet']['thumbnails']['default']['url'],
                        **stats
                    })
                
                # Get the next page of videos
                request = self.youtube.playlistItems().list_next(request, response)
                
                if len(videos) >= max_results:
                    break
            
            return videos[:max_results]
        except HttpError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e

    def get_channel_id(self, channel_url_or_username: str) -> str:
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
            # If it's already a channel ID, return it
            if channel_url_or_username.startswith('UC') and len(channel_url_or_username) == 24:
                return channel_url_or_username
            
            # Extract username/handle from URL if needed
            if '/' in channel_url_or_username:
                parts = channel_url_or_username.strip('/').split('/')
                channel_url_or_username = parts[-1]
                if channel_url_or_username.startswith('@'):
                    # Remove @ from handle
                    channel_url_or_username = channel_url_or_username[1:]
            
            # Try to find by username first
            try:
                request = self.youtube.channels().list(
                    part="id",
                    forUsername=channel_url_or_username
                )
                response = request.execute()
                
                if response['items']:
                    return response['items'][0]['id']
            except:
                pass
            
            # If username search fails, try search
            request = self.youtube.search().list(
                part="id",
                q=channel_url_or_username,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            
            if not response['items']:
                raise YouTubeAPIError(f"Channel not found: {channel_url_or_username}")
                
            return response['items'][0]['id']['channelId']
            
        except HttpError as e:
            error_msg = f"YouTube API error: {str(e)}"
            self.logger.error(error_msg)
            raise YouTubeAPIError(error_msg) from e
