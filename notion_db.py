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
            youtube = await self.notion_youtube
            db_name = os.getenv('NOTION_DB_YT_CHANNEL')
            
            self.logger.debug(f"Updating YouTube stats with page_id={youtube['page_id']} and db_name={db_name}")
            
            if not db_name:
                raise NotionDBError("NOTION_DB_YT_CHANNEL environment variable is not set")
            
            # Find the database
            db = await self.find_database(db_name, parent_id=youtube['page_id'])
            
            # Clear existing entries
            await self.clear_database(db['id'])
            
            # Format properties
            properties = {
                "Title": self.format_property(NotionPropertyType.TITLE, "Channel Stats"),
                "Subscribers": self.format_property(NotionPropertyType.NUMBER, stats.get('subscriberCount', 0)),
                "Videos": self.format_property(NotionPropertyType.NUMBER, stats.get('videoCount', 0)),
                "Views": self.format_property(NotionPropertyType.NUMBER, stats.get('viewCount', 0)),
                "Last Updated": self.format_property(NotionPropertyType.DATE, datetime.now().isoformat())
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
        else:
            raise ValueError(f"Unsupported property type: {prop_type}")

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
                
            # Find exact match (case-insensitive)
            for page in results:
                title = self._extract_page_title(page)
                if title and title.lower() == page_name.lower():
                    return {
                        'id': page['id'],
                        'title': title,
                        'url': page['url'],
                        'parent': page.get('parent', {}),
                        'created_time': page['created_time'],
                        'last_edited_time': page['last_edited_time']
                    }
            
            raise NotionDBError(f"Could not find exact match for page: {page_name}")
            
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
                
            # Find exact match (case-insensitive) with optional parent check
            for db in results:
                title = self._extract_page_title(db)
                if title and title.lower() == database_name.lower():
                    # If parent_id is specified, check if this database belongs to that parent
                    if parent_id:
                        db_parent = db.get('parent', {})
                        if db_parent.get('type') == 'page_id' and db_parent.get('page_id') == parent_id:
                            return self._format_database_info(db)
                    else:
                        return self._format_database_info(db)
            
            parent_msg = f" in parent page {parent_id}" if parent_id else ""
            raise NotionDBError(f"Could not find exact match for database: {database_name}{parent_msg}")
            
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
            'title': self._extract_page_title(db),
            'url': db['url'],
            'parent': db.get('parent', {}),
            'created_time': db['created_time'],
            'last_edited_time': db['last_edited_time'],
            'properties': db.get('properties', {})
        }
