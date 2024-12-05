import logging
import os
import json
from enum import Enum
import aiohttp
from typing import Dict, List, Optional, Any, Union
from datetime import datetime


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

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, endpoint: str, data: Optional[dict] = None, params: Optional[dict] = None) -> dict:
        """Make an HTTP request to the Notion API"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)

        url = f"{self.base_url}/{endpoint}"
        try:
            async with self.session.request(method, url, json=data, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            self.logger.error(f"Error making request to Notion API: {str(e)}")
            raise NotionDBError(f"Error making request to Notion API: {str(e)}") from e

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

    async def get_pages(self, include_children: bool = False) -> Dict[str, Dict]:
        """Get pages accessible to the current connection.
        
        Args:
            include_children (bool): If True, includes child pages in a 'children' field
        
        Returns:
            Dict[str, Dict]: Dictionary of pages with title as key and page details as value
        """
        try:
            response = await self._make_request(
                "POST", 
                "search", 
                {
                    "filter": {
                        "property": "object",
                        "value": "page"
                    },
                    "filter_properties": ["parent"]
                }
            )
            pages = {}
            for page in response.get('results', []):
                # Only include pages that are workspace pages or database pages
                parent = page.get('parent', {})
                parent_type = parent.get('type')
                if parent_type not in ('workspace', 'database'):
                    continue
                    
                title = self._extract_page_title(page)
                if not title:
                    continue
                    
                page_info = {
                    'id': page['id'],
                    'url': page['url'],
                    'created_time': page['created_time'],
                    'last_edited_time': page['last_edited_time'],
                    'parent': page.get('parent', {}),
                    'archived': page.get('archived', False),
                    'icon': page.get('icon', {}),
                    'cover': page.get('cover', {}),
                    'properties': page.get('properties', {}),
                }
                
                if include_children:
                    children = await self._get_child_pages(page['id'])
                    if children:
                        page_info['children'] = children
                
                # Log full page object for inspection
                self.logger.debug(f"Full page object for {title}: {json.dumps(page, indent=2)}")
                
                pages[title] = page_info
            return pages
        except Exception as e:
            self.logger.error(f"Error getting pages: {str(e)}")
            return {}
            
    async def _get_child_pages(self, page_id: str) -> Dict[str, Dict]:
        """Get child pages for a given page ID.
        
        Args:
            page_id (str): The parent page ID
            
        Returns:
            Dict[str, Dict]: Dictionary of child pages
        """
        try:
            response = await self._make_request(
                "GET", 
                f"blocks/{page_id}/children",
                params={"page_size": 100}
            )
            
            children = {}
            for block in response.get('results', []):
                if block['type'] == 'child_page':
                    title = block.get('child_page', {}).get('title', '')
                    if title:
                        children[title] = {
                            'id': block['id'],
                            'created_time': block['created_time'],
                            'last_edited_time': block['last_edited_time']
                        }
            return children
        except Exception as e:
            self.logger.error(f"Error getting child pages: {str(e)}")
            return {}

    def _extract_page_title(self, page: Dict) -> Optional[str]:
        """Extract the title from a page object.
        
        Args:
            page (Dict): The page object from Notion API
            
        Returns:
            Optional[str]: The page title or None if not found
        """
        properties = page.get('properties', {})
        # Try to find title in properties
        for prop in properties.values():
            if prop['type'] == 'title':
                title_items = prop.get('title', [])
                if title_items:
                    return title_items[0].get('plain_text', '')
        
        # Fallback to icon and emoji if no title found
        icon = page.get('icon', {})
        if icon and icon.get('type') == 'emoji':
            return icon.get('emoji', '')
            
        return None

    async def update_youtube_channel_stats(self, stats: Dict[str, Any]) -> None:
        """Update YouTube channel stats in Notion database"""
        try:
            youtube = await self.notion_youtube
            db_name = os.getenv('NOTION_DB_YT_CHANNEL')
            
            self.logger.debug(f"Updating YouTube stats with page_id={youtube['page_id']} and db_name={db_name}")
            
            if not db_name:
                raise NotionDBError("NOTION_DB_YT_CHANNEL environment variable is not set")
            
            # Get child databases
            child_dbs = await self.get_child_databases(page_id=youtube['page_id'])
            db_id = child_dbs[db_name]['id']
            
            # Clear existing entries
            await self.clear_database(db_id)
            
            # Format properties
            properties = {
                "Name": self.format_property(NotionPropertyType.TITLE, "A Measure of Passion"),
                "Subscribers": self.format_property(NotionPropertyType.NUMBER, stats.get('subscriberCount', 0)),
                "Views": self.format_property(NotionPropertyType.NUMBER, stats.get('viewCount', 0)),
                "Videos": self.format_property(NotionPropertyType.NUMBER, stats.get('videoCount', 0)),
                "Last Updated": self.format_property(NotionPropertyType.DATE, datetime.now().isoformat()),
            }
            
            # Create new entry
            await self.create_or_update_row(database_id=db_id, properties=properties)
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
        else:
            raise ValueError(f"Unsupported property type: {prop_type}")
