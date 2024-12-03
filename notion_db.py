import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from notion_client import Client
from notion_client.errors import APIResponseError
from enum import Enum, auto

class NotionDBError(Exception):
    """Base exception for Notion DB operations"""
    pass

class NotionPropertyType(Enum):
    """Enum for Notion property types"""
    TITLE = "title"
    RICH_TEXT = "rich_text"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    NUMBER = "number"
    CHECKBOX = "checkbox"
    DATE = "date"
    URL = "url"
    EMAIL = "email"
    PHONE_NUMBER = "phone_number"
    STATUS = "status"
    FILES = "files"

class NotionDB:
    def __init__(self, token: str, log_level: int = logging.INFO, logger: Optional[logging.Logger] = None):
        """
        Initialize Notion DB client
        
        Args:
            token (str): Notion integration token
            log_level (int): Logging level (default: logging.INFO)
            logger (Optional[logging.Logger]): Custom logger instance
        """
        self.client = Client(auth=token)
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
        self.logger.info("Notion DB client initialized successfully")
    
    def get_database_schema(self, database_id: str) -> Dict[str, Any]:
        """
        Retrieve the schema of a Notion database
        
        Args:
            database_id (str): ID of the database
            
        Returns:
            Dict[str, Any]: Database schema
            
        Raises:
            NotionDBError: If database retrieval fails
        """
        try:
            self.logger.debug(f"Getting schema for database {database_id}")
            response = self.client.databases.retrieve(database_id=database_id)
            self.logger.debug(f"Retrieved schema for database {database_id}")
            return response['properties']
        except APIResponseError as e:
            error_msg = f"Failed to retrieve database schema: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def delete_database(self, database_id: str) -> bool:
        """
        Delete a Notion database
        
        Args:
            database_id (str): ID of the database to delete
            
        Returns:
            bool: True if successful
            
        Raises:
            NotionDBError: If deletion fails
        """
        try:
            self.logger.debug(f"Deleting database {database_id}")
            self.client.blocks.delete(block_id=database_id)
            self.logger.info(f"Successfully deleted database {database_id}")
            return True
        except APIResponseError as e:
            error_msg = f"Failed to delete database: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def create_database(self, parent_page_id: str, title: str, schema: Dict[str, Any]) -> str:
        """
        Create a new Notion database
        
        Args:
            parent_page_id (str): ID of the parent page
            title (str): Title of the database
            schema (Dict[str, Any]): Database schema definition
            
        Returns:
            str: ID of the created database
            
        Raises:
            NotionDBError: If database creation fails
        """
        try:
            self.logger.debug(f"Creating database {title} in page {parent_page_id}")
            response = self.client.databases.create(
                parent={"type": "page_id", "page_id": parent_page_id},
                title=[{"type": "text", "text": {"content": title}}],
                properties=schema
            )
            database_id = response['id']
            self.logger.info(f"Created database {title} with ID {database_id}")
            return database_id
        except APIResponseError as e:
            error_msg = f"Failed to create database: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def create_or_update_row(self, database_id: str, properties: Dict[str, Any], 
                           page_id: Optional[str] = None) -> str:
        """
        Create or update a row in a Notion database
        
        Args:
            database_id (str): ID of the database
            properties (Dict[str, Any]): Row properties
            page_id (Optional[str]): ID of existing page to update
            
        Returns:
            str: ID of the created/updated page
            
        Raises:
            NotionDBError: If operation fails
        """
        try:
            if page_id:
                self.logger.debug(f"Updating row {page_id} in database {database_id}")
                response = self.client.pages.update(
                    page_id=page_id,
                    properties=properties
                )
                self.logger.debug(f"Updated row {page_id} in database {database_id}")
            else:
                self.logger.debug(f"Creating new row in database {database_id}")
                response = self.client.pages.create(
                    parent={"database_id": database_id},
                    properties=properties
                )
                self.logger.debug(f"Created new row in database {database_id}")
            
            return response['id']
        except APIResponseError as e:
            error_msg = f"Failed to create/update row: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def get_row(self, page_id: str) -> Dict[str, Any]:
        """
        Fetch a specific row from a Notion database
        
        Args:
            page_id (str): ID of the page/row
            
        Returns:
            Dict[str, Any]: Row data
            
        Raises:
            NotionDBError: If fetch fails
        """
        try:
            self.logger.debug(f"Getting row {page_id}")
            response = self.client.pages.retrieve(page_id=page_id)
            self.logger.debug(f"Retrieved row {page_id}")
            return response['properties']
        except APIResponseError as e:
            error_msg = f"Failed to retrieve row: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def query_database(self, database_id: str, filter_obj: Optional[Dict[str, Any]] = None,
                      sorts: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Query a Notion database with optional filters and sorting
        
        Args:
            database_id (str): ID of the database
            filter_obj (Optional[Dict[str, Any]]): Filter conditions
            sorts (Optional[List[Dict[str, Any]]]): Sort conditions
            
        Returns:
            List[Dict[str, Any]]: List of matching rows
            
        Raises:
            NotionDBError: If query fails
        """
        try:
            self.logger.debug(f"Querying database {database_id}")
            query_params = {}
            if filter_obj:
                query_params['filter'] = filter_obj
            if sorts:
                query_params['sorts'] = sorts
                
            response = self.client.databases.query(
                database_id=database_id,
                **query_params
            )
            
            results = response['results']
            self.logger.debug(f"Retrieved {len(results)} rows from database {database_id}")
            return results
        except APIResponseError as e:
            error_msg = f"Failed to query database: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
    
    def get_child_databases(self, page_id: str) -> List[Dict[str, Any]]:
        """
        Get all child databases in a page
        
        Args:
            page_id (str): Notion page ID
            
        Returns:
            List[Dict[str, Any]]: List of child databases with their properties
            
        Raises:
            NotionDBError: If API request fails
        """
        try:
            self.logger.debug(f"Getting child databases for page {page_id}")
            response = self.client.blocks.children.list(block_id=page_id)
            databases = []
            
            for block in response['results']:
                if block['type'] == 'child_database':
                    self.logger.debug(f"Found child database: {block['id']}")
                    db_info = self.client.databases.retrieve(database_id=block['id'])
                    databases.append({
                        'id': block['id'],
                        'title': block['child_database']['title'],
                        'properties': db_info['properties']
                    })
            
            self.logger.debug(f"Found {len(databases)} child databases")
            return databases
            
        except APIResponseError as e:
            error_msg = f"Failed to get child databases: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e

    def clear_database(self, database_id: str) -> int:
        """
        Remove all entries from a Notion database
        
        Args:
            database_id (str): ID of the database to clear
            
        Returns:
            int: Number of entries deleted
            
        Raises:
            NotionDBError: If deletion fails
        """
        try:
            self.logger.debug(f"Clearing database {database_id}")
            deleted_count = 0
            has_more = True
            start_cursor = None
            
            while has_more:
                # Query database for pages, 100 at a time
                response = self.client.databases.query(
                    database_id=database_id,
                    start_cursor=start_cursor,
                    page_size=100
                )
                
                # Delete each page
                for page in response.get('results', []):
                    try:
                        self.client.pages.update(
                            page_id=page['id'],
                            archived=True  # This soft-deletes the page
                        )
                        deleted_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to delete page {page['id']}: {str(e)}")
                
                # Check if there are more pages to process
                has_more = response.get('has_more', False)
                start_cursor = response.get('next_cursor')
            
            self.logger.info(f"Successfully cleared {deleted_count} entries from database {database_id}")
            return deleted_count
            
        except Exception as e:
            error_msg = f"Failed to clear database {database_id}: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e

    def format_property(self, property_type: NotionPropertyType, value: Any) -> Dict[str, Any]:
        """
        Format a value according to its Notion property type
        
        Args:
            property_type (NotionPropertyType): The Notion property type enum
            value (Any): The value to format. For files, can be a string URL or a list of URLs
            
        Returns:
            Dict[str, Any]: Formatted property ready for Notion API
            
        Raises:
            NotionDBError: If property type is not supported or formatting fails
        """
        try:
            if property_type == NotionPropertyType.TITLE:
                return {
                    "title": [{"text": {"content": str(value)}}]
                }
            elif property_type == NotionPropertyType.RICH_TEXT:
                return {
                    "rich_text": [{"text": {"content": str(value)}}]
                }
            elif property_type == NotionPropertyType.SELECT:
                return {
                    "select": {"name": str(value)}
                }
            elif property_type == NotionPropertyType.MULTI_SELECT:
                # Handle both string and list inputs
                values = value if isinstance(value, list) else [value]
                return {
                    "multi_select": [{"name": str(v)} for v in values]
                }
            elif property_type == NotionPropertyType.NUMBER:
                return {
                    "number": float(value)
                }
            elif property_type == NotionPropertyType.CHECKBOX:
                return {
                    "checkbox": bool(value)
                }
            elif property_type == NotionPropertyType.DATE:
                # Handle datetime objects or ISO format strings
                if isinstance(value, datetime):
                    date_str = value.isoformat()
                else:
                    date_str = str(value)
                return {
                    "date": {"start": date_str}
                }
            elif property_type == NotionPropertyType.URL:
                return {
                    "url": str(value)
                }
            elif property_type == NotionPropertyType.EMAIL:
                return {
                    "email": str(value)
                }
            elif property_type == NotionPropertyType.PHONE_NUMBER:
                return {
                    "phone_number": str(value)
                }
            elif property_type == NotionPropertyType.STATUS:
                return {
                    "status": {"name": str(value)}
                }
            elif property_type == NotionPropertyType.FILES:
                # Handle both single URL and list of URLs
                files = value if isinstance(value, list) else [value]
                return {
                    "files": [
                        {
                            "type": "external",
                            "name": url.split('/')[-1],  # Use filename from URL as name
                            "external": {"url": url}
                        }
                        for url in files
                    ]
                }
            else:
                raise NotionDBError(f"Unsupported property type: {property_type}")
        except Exception as e:
            error_msg = f"Failed to format property of type {property_type}: {str(e)}"
            self.logger.error(error_msg)
            raise NotionDBError(error_msg) from e
