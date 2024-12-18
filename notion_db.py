import logging
import os
import json
from enum import Enum
import aiohttp
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import asyncio
from asyncio import Lock, Semaphore
import time

class NotionPropertyType(Enum):
    TITLE = "title"
    RICH_TEXT = "rich_text"
    NUMBER = "number"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    DATE = "date"
    PEOPLE = "people"
    FILES = "files"
    CHECKBOX = "checkbox"
    URL = "url"
    EMAIL = "email"
    PHONE_NUMBER = "phone_number"
    FORMULA = "formula"
    RELATION = "relation"
    ROLLUP = "rollup"
    CREATED_TIME = "created_time"
    CREATED_BY = "created_by"
    LAST_EDITED_TIME = "last_edited_time"
    LAST_EDITED_BY = "last_edited_by"


class NotionDBError(Exception):
    """Base exception for Notion DB operations"""
    pass


class NotionDB:
    def __init__(self, token: str, logger: logging.Logger, log_level: int = logging.INFO):
        self.token = token
        self.logger = logger
        self.logger.setLevel(log_level)
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.base_url = "https://api.notion.com/v1"
        self.session = None
        self._page_cache = {}  # Cache for page info
        self._db_cache = {}  # Cache for database info
        
        # Rate limiting
        self.request_semaphore = Semaphore(3)  # Max 3 concurrent requests
        self.last_request_time = 0
        self.min_request_interval = 0.34  # ~3 requests per second
        self.request_lock = Lock()
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _wait_for_rate_limit(self):
        """Wait if needed to respect rate limits"""
        async with self.request_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_request_interval:
                await asyncio.sleep(self.min_request_interval - time_since_last)
            self.last_request_time = time.time()

    async def _make_request(self, method: str, endpoint: str, data: Optional[dict] = None, params: Optional[dict] = None) -> dict:
        """Make an HTTP request to the Notion API with rate limiting and retries"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)

        url = f"{self.base_url}/{endpoint}"
        max_retries = 3
        base_delay = 1  # Start with 1 second delay
        
        async with self.request_semaphore:  # Limit concurrent requests
            for attempt in range(max_retries):
                try:
                    await self._wait_for_rate_limit()
                    async with self.session.request(method, url, json=data, params=params) as response:
                        if response.status == 429:  # Too Many Requests
                            retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                            self.logger.warning(f"Rate limited. Waiting {retry_after} seconds before retry.")
                            await asyncio.sleep(retry_after)
                            continue
                            
                        if response.status == 400:  # Bad Request
                            error_body = await response.json()
                            error_msg = error_body.get('message', 'Unknown error')
                            self.logger.error(f"Bad request: {error_msg}")
                            raise NotionDBError(f"Bad request: {error_msg}")
                            
                        response.raise_for_status()
                        return await response.json()
                        
                except aiohttp.ClientError as e:
                    if attempt == max_retries - 1:  # Last attempt
                        self.logger.error(f"Error making request to Notion API: {str(e)}")
                        raise NotionDBError(f"Error making request to Notion API: {str(e)}") from e
                    
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(f"Request failed. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

    async def get_database(self, database_id: str) -> dict:
        """Get a database by ID"""
        return await self._make_request("GET", f"databases/{database_id}")

    async def get_database_by_name(self, database_name: str, page_id: Optional[str] = None) -> Optional[dict]:
        """Get a database by name from a parent page"""
        try:
            databases = await self.get_child_databases(page_id) if page_id else await self.search_databases()
            return next((db for db in databases if db['title'] == database_name), None)
        except Exception as e:
            self.logger.error(f"Error getting database by name: {str(e)}")
            raise NotionDBError(f"Error getting database by name: {str(e)}") from e

    async def get_child_databases(self, page_id: str) -> Dict[str, dict]:
        """Get all child databases of a page"""
        self.logger.debug(f"Getting child databases for page ID: {page_id}")
        if not page_id:
            raise NotionDBError("Page ID cannot be None or empty")
            
        try:
            results = await self._make_request("GET", f"blocks/{page_id}/children")
            databases = {}
            for block in results.get('results', []):
                if block['type'] == 'child_database':
                    db_id = block['id']
                    db_info = await self.get_database(db_id)
                    title = db_info['title'][0]['plain_text'] if db_info.get('title') else ''
                    databases[title] = {'id': db_id, 'title': title}
            self.logger.debug(f"Found {len(databases)} child databases")
            return databases
        except Exception as e:
            self.logger.error(f"Error getting child databases for page {page_id}: {str(e)}")
            raise NotionDBError(f"Error getting child databases: {str(e)}") from e

    async def search_databases(self) -> List[dict]:
        """Search for databases"""
        results = await self._make_request("POST", "search", {"filter": {"property": "object", "value": "database"}})
        return results.get('results', [])

    async def query_database(self, database_id: str, filter_params: Optional[dict] = None) -> List[dict]:
        """Query a database with optional filters"""
        data = {"filter": filter_params} if filter_params else {}
        results = await self._make_request("POST", f"databases/{database_id}/query", data)
        return results.get('results', [])

    async def create_page(self, database_id: str, properties: dict) -> dict:
        """Create a new page in a database"""
        data = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        return await self._make_request("POST", "pages", data)

    async def update_page(self, page_id: str, properties: dict) -> dict:
        """Update a page's properties"""
        return await self._make_request("PATCH", f"pages/{page_id}", {"properties": properties})

    async def delete_page(self, page_id: str) -> dict:
        """Archive a page"""
        return await self._make_request("PATCH", f"pages/{page_id}", {"archived": True})

    async def clear_database(self, database_id: str) -> None:
        """Clear all entries in a database"""
        try:
            pages = await self.query_database(database_id)
            for page in pages:
                await self.delete_page(page['id'])
        except Exception as e:
            self.logger.error(f"Error clearing database: {str(e)}")
            raise NotionDBError(f"Error clearing database: {str(e)}") from e

    async def create_or_update_row(self, database_id: str, properties: dict, filter_params: Optional[dict] = None) -> dict:
        """Create a new row or update existing one if it matches the filter"""
        try:
            if filter_params:
                existing_pages = await self.query_database(database_id, filter_params)
                if existing_pages:
                    return await self.update_page(existing_pages[0]['id'], properties)
            return await self.create_page(database_id, properties)
        except Exception as e:
            self.logger.error(f"Error creating/updating row: {str(e)}")
            raise NotionDBError(f"Error creating/updating row: {str(e)}") from e

    async def delete_pages_where(self, database_id: str, filter_params: dict) -> int:
        """Delete pages in a database that match the filter criteria
        
        Args:
            database_id (str): ID of the database
            filter_params (dict): Filter parameters in Notion API format
            
        Returns:
            int: Number of pages deleted
            
        Example:
            # Delete pages where "Date" is before 2024-01-01
            filter_params = {
                "property": "Date",
                "date": {
                    "before": "2024-01-01"
                }
            }
        """
        try:
            # Query database with filter
            pages = await self.query_database(database_id, filter_params)
            
            # Delete matching pages
            deleted_count = 0
            for page in pages:
                await self.delete_page(page['id'])
                deleted_count += 1
            
            self.logger.debug(f"Deleted {deleted_count} pages matching filter in database {database_id}")
            return deleted_count
        except Exception as e:
            self.logger.error(f"Error deleting pages with filter: {str(e)}")
            raise NotionDBError(f"Error deleting pages with filter: {str(e)}") from e

    def _extract_page_title(self, page: dict) -> Optional[str]:
        """Extract title from page object"""
        properties = page.get('properties', {})
        title_prop = None
        
        # First try to find a property of type "title"
        for prop in properties.values():
            if prop.get('type') == 'title':
                title_prop = prop
                break
                
        # If no title property found, try to get it from child_page title
        if not title_prop and page.get('type') == 'child_page':
            return page.get('child_page', {}).get('title', '')
            
        if not title_prop:
            return None
            
        title_content = title_prop.get('title', [])
        if not title_content:
            return None
            
        return title_content[0].get('plain_text', '')

    async def update_youtube_channel_stats(self, stats: Dict[str, Any]) -> None:
        """Update YouTube channel stats in Notion database"""
        try:
            db = await self.notion_db_yt_channel
            
            self.logger.debug(f"Updating YouTube stats in database {db['title']}")
            
            # Clear existing entries
            await self.clear_database(db['id'])
            
            # Format properties with correct property names
            properties = {
                "Name": self.format_property(NotionPropertyType.TITLE, "Channel Stats"),
                "Subscriber Count": self.format_property(NotionPropertyType.NUMBER, stats.get('subscriberCount', 0)),
                "Video Count": self.format_property(NotionPropertyType.NUMBER, stats.get('videoCount', 0)),
                "View Count": self.format_property(NotionPropertyType.NUMBER, stats.get('viewCount', 0)),
                "Updated At": self.format_property(NotionPropertyType.DATE, datetime.now().isoformat())
            }
            
            # Create new entry
            await self.create_or_update_row(database_id=db['id'], properties=properties)
            self.logger.info("Updated YouTube channel stats in Notion")
        except Exception as e:
            self.logger.error(f"Error updating YouTube channel stats: {str(e)}")
            raise

    @staticmethod
    def format_property(prop_type: NotionPropertyType, value: Any) -> dict:
        """Format a value for a specific Notion property type"""
        if prop_type == NotionPropertyType.TITLE:
            return {"title": [{"text": {"content": str(value)}}]}
        elif prop_type == NotionPropertyType.RICH_TEXT:
            return {"rich_text": [{"text": {"content": str(value)}}]}
        elif prop_type == NotionPropertyType.NUMBER:
            return {"number": float(value) if value is not None else None}
        elif prop_type == NotionPropertyType.SELECT:
            return {"select": {"name": str(value)}}
        elif prop_type == NotionPropertyType.MULTI_SELECT:
            return {"multi_select": [{"name": str(item)} for item in value]}
        elif prop_type == NotionPropertyType.DATE:
            return {"date": {"start": str(value)}}
        elif prop_type == NotionPropertyType.CHECKBOX:
            return {"checkbox": bool(value)}
        elif prop_type == NotionPropertyType.URL:
            return {"url": str(value)}
        elif prop_type == NotionPropertyType.FILES:
            if isinstance(value, dict) and 'url' in value:
                # If just given a URL, format it as an external file
                return {"files": [{
                    "type": "external",
                    "name": value.get('name', 'External File'),
                    "external": {
                        "url": value['url']
                    }
                }]}
            # Otherwise assume it's already in the correct format
            if not isinstance(value, list):
                value = [value]
            return {"files": value}
        else:
            raise ValueError(f"Unsupported property type: {prop_type}")

    async def get_page_info(self, page_name: str) -> Dict[str, Any]:
        """Get cached page info including its databases.
        
        Args:
            page_name (str): Name of the page as configured in environment variables
            
        Returns:
            Dict containing page info with 'name', 'page_id', and 'database_ids'
            
        Raises:
            NotionDBError: If page cannot be found
        """
        # Check cache first
        if page_name in self._page_cache:
            return self._page_cache[page_name]
            
        # Get page info
        page_info = {"name": page_name}
        page = await self.find_page(page_name)
        page_info["page_id"] = page['id']
        page_info["database_ids"] = await self.get_child_databases(page_id=page_info["page_id"])
        
        # Cache the result
        self._page_cache[page_name] = page_info
        return page_info
        
    @property
    async def notion_page_telly(self) -> Dict[str, Any]:
        """Get cached Telly page info"""
        page_name = os.getenv('NOTION_PAGE_TELLY')
        if not page_name:
            raise NotionDBError("NOTION_PAGE_TELLY environment variable is not set")
        return await self.get_page_info(page_name)
        
    @property
    async def notion_page_youtube(self) -> Dict[str, Any]:
        """Get cached YouTube page info"""
        page_name = os.getenv('NOTION_PAGE_YOUTUBE')
        if not page_name:
            raise NotionDBError("NOTION_PAGE_YOUTUBE environment variable is not set")
        return await self.get_page_info(page_name)

    def _format_page_info(self, page: Dict[str, Any], query_name: Optional[str] = None) -> Dict[str, Any]:
        """Format page information into a consistent structure.
        
        Args:
            page (Dict[str, Any]): Raw page object from Notion API
            query_name (Optional[str]): Name used in the query, if available
            
        Returns:
            Dict[str, Any]: Formatted page information
        """
        return {
            'id': page['id'],
            'title': query_name or self._extract_page_title(page),
            'url': page['url'],
            'parent': page.get('parent', {}),
            'created_time': page['created_time'],
            'last_edited_time': page['last_edited_time']
        }

    async def find_page(self, page_name: str) -> Dict[str, Any]:
        """Find a specific page by name using Notion's search.
        
        Args:
            page_name (str): Name of the page to find
            
        Returns:
            Dict containing page info with id, title, and other metadata
            
        Raises:
            NotionDBError: If page cannot be found
        """
        try:
            response = await self._make_request(
                "POST", 
                "search", 
                {
                    "query": page_name,
                    "filter": {
                        "value": "page",
                        "property": "object"
                    },
                    "sort": {
                        "direction": "ascending",
                        "timestamp": "last_edited_time"
                    }
                }
            )
            
            results = response.get('results', [])
            if not results:
                raise NotionDBError(f"Could not find page with name: {page_name}")
            
            # Return first result since we already filtered by name in search
            page = results[0]
            return self._format_page_info(page, page_name)
            
        except Exception as e:
            self.logger.error(f"Error finding page {page_name}: {str(e)}")
            raise NotionDBError(f"Error finding page {page_name}: {str(e)}") from e

    async def find_database(self, database_name: str, parent_id: Optional[str] = None) -> Dict[str, Any]:
        """Find a specific database by name using Notion's search.
        
        Args:
            database_name (str): Name of the database to find
            parent_id (Optional[str]): Parent page ID to scope the search
            
        Returns:
            Dict containing database info with id, title, and other metadata
            
        Raises:
            NotionDBError: If database cannot be found
        """
        try:
            search_params = {
                "query": database_name,
                "filter": {
                    "value": "database",
                    "property": "object"
                },
                "sort": {
                    "direction": "ascending",
                    "timestamp": "last_edited_time"
                }
            }
            
            response = await self._make_request("POST", "search", search_params)
            results = response.get('results', [])
            if not results:
                raise NotionDBError(f"Could not find database with name: {database_name}")
            
            # Return first result since we already filtered by name in search
            # If parent_id is specified, find the first database with matching parent
            for db in results:
                if parent_id:
                    db_parent = db.get('parent', {})
                    if db_parent.get('type') == 'page_id' and db_parent.get('page_id') == parent_id:
                        db['_query_name'] = database_name  # Add query name to db object
                        return self._format_database_info(db)
                else:
                    db['_query_name'] = database_name  # Add query name to db object
                    return self._format_database_info(db)
            
            if parent_id:
                raise NotionDBError(f"Could not find database with name: {database_name} under parent: {parent_id}")
            
        except Exception as e:
            self.logger.error(f"Error finding database {database_name}: {str(e)}")
            raise NotionDBError(f"Error finding database {database_name}: {str(e)}") from e

    def _format_database_info(self, db: Dict[str, Any]) -> Dict[str, Any]:
        """Format database information into a consistent structure.
        
        Args:
            db (Dict[str, Any]): Raw database object from Notion API
            
        Returns:
            Dict[str, Any]: Formatted database information
        """
        return {
            'id': db['id'],
            'title': db.get('_query_name', self._extract_page_title(db)),  # Fallback to extracted title if query name not provided
            'url': db['url'],
            'parent': db.get('parent', {}),
            'created_time': db['created_time'],
            'last_edited_time': db['last_edited_time'],
            'properties': db.get('properties', {})
        }

    @property
    async def notion_db_tv_calendar(self) -> Dict[str, Any]:
        """Get cached TV Calendar database info"""
        db_name = os.getenv('NOTION_DB_TV_CALENDAR')
        if not db_name:
            raise NotionDBError("NOTION_DB_TV_CALENDAR environment variable is not set")
        return await self.get_database_info(db_name, os.getenv('NOTION_PAGE_TELLY'))

    @property
    async def notion_db_yt_channel(self) -> Dict[str, Any]:
        """Get cached YouTube Channel database info"""
        db_name = os.getenv('NOTION_DB_YT_CHANNEL')
        if not db_name:
            raise NotionDBError("NOTION_DB_YT_CHANNEL environment variable is not set")
        return await self.get_database_info(db_name, os.getenv('NOTION_PAGE_YOUTUBE'))

    async def get_database_info(self, db_name: str, parent_page_name: Optional[str] = None) -> Dict[str, Any]:
        """Get cached database info.
        
        Args:
            db_name (str): Name of the database as configured in environment variables
            parent_page_name (Optional[str]): Name of the parent page if database is a child
            
        Returns:
            Dict containing database info with 'name', 'id', and other metadata
            
        Raises:
            NotionDBError: If database cannot be found
        """
        # Check cache first
        cache_key = f"{parent_page_name}_{db_name}" if parent_page_name else db_name
        if cache_key in self._db_cache:
            return self._db_cache[cache_key]
        
        # Get parent page ID if specified
        parent_id = None
        if parent_page_name:
            parent_info = await self.get_page_info(parent_page_name)
            parent_id = parent_info['page_id']
        
        # Find database
        db_info = await self.find_database(db_name, parent_id)
        
        # Cache the result
        self._db_cache[cache_key] = db_info
        return db_info

    async def batch_update_pages(self, updates: List[Tuple[str, dict]]) -> List[dict]:
        """Batch update multiple pages with rate limiting
        
        Args:
            updates: List of (page_id, properties) tuples
            
        Returns:
            List of updated page objects
        """
        results = []
        for page_id, properties in updates:
            try:
                result = await self.update_page(page_id, properties)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error updating page {page_id}: {str(e)}")
                results.append({"id": page_id, "error": str(e)})
        return results

    async def batch_create_pages(self, database_id: str, pages: List[dict]) -> List[dict]:
        """Batch create multiple pages with rate limiting
        
        Args:
            database_id: ID of the database to create pages in
            pages: List of page property dictionaries
            
        Returns:
            List of created page objects
        """
        results = []
        for properties in pages:
            try:
                result = await self.create_page(database_id, properties)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error creating page: {str(e)}")
                results.append({"error": str(e)})
        return results
