"""
Omeka S API client module for fetching and processing items from an Omeka S instance.
This module provides a robust interface for interacting with the Omeka S API,
including data fetching, caching, and error handling capabilities.
"""

import os
import json
import logging
from enum import Enum
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Iterator, Tuple
from pathlib import Path
from datetime import datetime

# Set up logging
def setup_logging(log_file='omeka_client.log'):
    """
    Configure logging with both file and console handlers.
    
    Args:
        log_file (str): Name of the log file to write to
        
    Returns:
        logging.Logger: Configured logger instance
    """
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logs directory if it doesn't exist
    log_dir = Path('/app/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('omeka_client')
    logger.setLevel(getattr(logging, log_level))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(log_dir / log_file)
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

@dataclass
class OmekaItem:
    """
    Data model representing an Omeka S item with its essential metadata.
    
    This class encapsulates the relevant metadata from an Omeka S item,
    providing a clean interface for accessing item properties.
    
    Attributes:
        id (int): Unique identifier of the item
        title (str): Title of the item
        resource_class_id (Optional[int]): ID of the item's resource class
        resource_class_label (Optional[str]): Label of the item's resource class
        created_date (datetime): Date when the item was created
        publication_date (Optional[str]): Date when the item was published
        num_pages (Optional[int]): Number of pages in the item
        language (Optional[str]): Language of the item
        word_count (Optional[int]): Total word count of the item's content
        item_set_ids (List[int]): List of item set IDs this item belongs to
        item_set_title (Optional[str]): Title of the item set this item belongs to
        country (Optional[str]): Country associated with the item
        type (Optional[str]): Display title from dcterms:type
    """
    
    id: int
    title: str
    resource_class_id: Optional[int]
    resource_class_label: Optional[str]
    created_date: datetime
    publication_date: Optional[str]
    num_pages: Optional[int]
    language: Optional[str]
    word_count: Optional[int]
    item_set_ids: List[int]
    item_set_title: Optional[str]
    country: Optional[str]
    type: Optional[str]
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], resource_class_labels: Dict[int, str] = None, 
                         item_set_titles: Dict[int, str] = None, item_set_countries: Dict[int, str] = None) -> 'OmekaItem':
        """
        Create an OmekaItem instance from raw API response data.
        
        Args:
            data (Dict[str, Any]): Raw item data from the Omeka S API
            resource_class_labels (Dict[int, str]): Mapping of resource class IDs to labels
            item_set_titles (Dict[int, str]): Mapping of item set IDs to titles
            item_set_countries (Dict[int, str]): Mapping of item set IDs to countries
            
        Returns:
            OmekaItem: Processed item instance with extracted metadata
            
        Raises:
            Exception: If required data is missing or invalid
        """
        try:
            # Extract title
            title = data.get('o:title', '')
            logger.debug(f"Processing item {data.get('o:id')}: {title}")
            
            # Extract resource class ID and label
            resource_class = data.get('o:resource_class', {})
            resource_class_id = resource_class.get('o:id') if resource_class else None
            resource_class_label = resource_class_labels.get(resource_class_id) if resource_class_labels else None
            
            # Extract created date
            created_str = data.get('o:created', {}).get('@value')
            created_date = datetime.fromisoformat(created_str.replace('Z', '+00:00')) if created_str else None
            
            # Extract publication date
            pub_dates = data.get('dcterms:date', [])
            publication_date = pub_dates[0].get('@value') if pub_dates else None
            
            # Extract number of pages
            num_pages_data = data.get('bibo:numPages', [])
            num_pages = int(num_pages_data[0].get('@value')) if num_pages_data else None
            
            # Extract language
            language_data = data.get('dcterms:language', [])
            language = language_data[0].get('display_title') if language_data else None
            
            # Calculate word count if content exists
            content_data = data.get('bibo:content', [])
            content = content_data[0].get('@value') if content_data else ''
            word_count = len(content.split()) if content else 0
            
            # Extract item set IDs, title and country
            item_sets = data.get('o:item_set', [])
            item_set_ids = [item_set.get('o:id') for item_set in item_sets if item_set.get('o:id')]
            item_set_title = None
            country = None
            
            if item_set_ids and item_set_titles:
                item_set_title = item_set_titles.get(item_set_ids[0])
            
            if item_set_countries and item_set_ids:
                country = item_set_countries.get(item_set_ids[0])

            # Extract type
            type_data = data.get('dcterms:type', [])
            type_value = type_data[0].get('display_title') if type_data else None
            
            return cls(
                id=data.get('o:id'),
                title=title,
                resource_class_id=resource_class_id,
                resource_class_label=resource_class_label,
                created_date=created_date,
                publication_date=publication_date,
                num_pages=num_pages,
                language=language,
                word_count=word_count,
                item_set_ids=item_set_ids,
                item_set_title=item_set_title,
                country=country,
                type=type_value
            )
        except Exception as e:
            logger.error(f"Error processing item {data.get('o:id')}: {str(e)}")
            raise

class ResourceType(Enum):
    """Enum for resource types to improve maintainability and type safety"""
    ITEM = 'items'
    ITEM_SET = 'item_sets'
    MEDIA = 'media'

@dataclass
class OmekaConfig:
    """Configuration class with validation and default values"""
    base_url: str = field(default_factory=lambda: os.getenv('OMEKA_BASE_URL', ''))
    key_identity: str = field(default_factory=lambda: os.getenv('IWAC_KEY_IDENTITY', ''))
    key_credential: str = field(default_factory=lambda: os.getenv('IWAC_KEY_CREDENTIAL', ''))
    cache_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent)

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not all([self.base_url, self.key_identity, self.key_credential]):
            raise ValueError("Missing required environment variables")
        
        # Ensure base_url doesn't end with trailing slash
        self.base_url = self.base_url.rstrip('/')
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

class ApiError(Exception):
    """Custom exception for API-related errors"""
    pass

class OmekaClient:
    """
    Enhanced Omeka S API client with caching and error handling capabilities.
    
    This client provides a robust interface for interacting with an Omeka S instance,
    including features like:
    - Automatic pagination handling
    - Caching of retrieved data
    - Error handling and retries
    - Progress tracking for long operations
    - Resource class and item set metadata resolution
    
    Attributes:
        config (OmekaConfig): Configuration for the Omeka S instance
        session (requests.Session): Configured HTTP session for API requests
        resource_class_labels (Dict[int, str]): Cache of resource class labels
        item_set_titles (Dict[int, str]): Cache of item set titles
        item_set_countries (Dict[int, str]): Cache of item set countries
    """
    
    def __init__(self, config: Optional[OmekaConfig] = None):
        """
        Initialize the Omeka S client.
        
        Args:
            config (Optional[OmekaConfig]): Configuration for the Omeka S instance.
                                          If None, loads from environment variables.
        """
        load_dotenv()
        self.config = config or OmekaConfig()
        self.session = self._create_session()
        self.resource_class_labels = {}
        self.item_set_titles = {}
        self.item_set_countries = {}

    def _create_session(self) -> requests.Session:
        """
        Create and configure a requests session with authentication.
        
        Returns:
            requests.Session: Configured session for API requests
        """
        session = requests.Session()
        session.params.update({
            'key_identity': self.config.key_identity,
            'key_credential': self.config.key_credential
        })
        return session

    def _get_total_items(self, resource_type: ResourceType) -> int:
        """
        Get the total number of items of a specific type.
        
        Args:
            resource_type (ResourceType): Type of resource to count
            
        Returns:
            int: Total number of items, or 0 if count cannot be determined
        """
        try:
            response = self.session.head(f"{self.config.base_url}/{resource_type.value}")
            return int(response.headers.get('Omeka-S-Total-Results', 0))
        except Exception as e:
            logger.warning(f"Could not get total items count: {str(e)}")
            return 0

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make an API request with retry logic and error handling.
        
        Args:
            endpoint (str): API endpoint to request
            params (Optional[Dict[str, Any]]): Query parameters for the request
            
        Returns:
            Any: Parsed JSON response from the API
            
        Raises:
            ApiError: If the request fails after all retries
        """
        url = f"{self.config.base_url}/{endpoint}"
        params = params or {}
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise ApiError(f"Failed to fetch data from {url}: {str(e)}")
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                continue
        return None

    def _paginated_fetch(self, resource_type: ResourceType, params: Dict[str, Any] = None) -> Iterator[OmekaItem]:
        """Generic paginated fetch method returning processed items"""
        params = params or {}
        page = 1
        per_page = 100
        total_items = self._get_total_items(resource_type)
        
        with tqdm(total=total_items, desc=f"Fetching {resource_type.value}", unit="item") as pbar:
            while True:
                params.update({'page': page, 'per_page': per_page})
                try:
                    data = self._make_request(resource_type.value, params)
                    if not data:
                        break
                        
                    for item_data in data:
                        try:
                            item = OmekaItem.from_api_response(item_data, self.resource_class_labels, self.item_set_titles, self.item_set_countries)
                            pbar.set_postfix({"Current": item.title[:30]}, refresh=True)
                            yield item
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Failed to process item on page {page}: {str(e)}")
                            continue
                            
                    page += 1
                except ApiError as e:
                    logger.error(f"Failed to fetch page {page}: {str(e)}")
                    break

    def _fetch_resource_class_label(self, resource_class_id: int) -> Optional[str]:
        """Fetch the label for a specific resource class ID"""
        try:
            data = self._make_request(f"resource_classes/{resource_class_id}")
            if data:
                return data.get('o:label')
        except ApiError as e:
            logger.warning(f"Failed to fetch resource class label for ID {resource_class_id}: {str(e)}")
        return None

    def _build_resource_class_labels(self, items: List[OmekaItem]):
        """Build a mapping of resource class IDs to labels"""
        unique_class_ids = {item.resource_class_id for item in items if item.resource_class_id}
        logger.info(f"Fetching labels for {len(unique_class_ids)} resource classes...")
        
        for class_id in tqdm(unique_class_ids, desc="Fetching resource class labels"):
            if class_id not in self.resource_class_labels:
                label = self._fetch_resource_class_label(class_id)
                if label:
                    self.resource_class_labels[class_id] = label

    def _fetch_item_set_metadata(self, item_set_id: int) -> Tuple[Optional[str], Optional[str]]:
        """Fetch both title and country for a specific item set ID"""
        try:
            data = self._make_request(f"item_sets/{item_set_id}")
            if data:
                title = data.get('o:title', '')
                # Extract country from spatial coverage
                spatial_data = data.get('dcterms:spatial', [])
                country = spatial_data[0].get('display_title') if spatial_data else None
                return title, country
        except ApiError as e:
            logger.warning(f"Failed to fetch item set metadata for ID {item_set_id}: {str(e)}")
        return None, None

    def _build_item_set_metadata(self, items: List[OmekaItem]):
        """Build mappings of item set IDs to titles and countries"""
        unique_set_ids = {id for item in items for id in item.item_set_ids}
        logger.info(f"Fetching metadata for {len(unique_set_ids)} item sets...")
        
        for set_id in tqdm(unique_set_ids, desc="Fetching item set metadata"):
            if set_id not in self.item_set_titles or set_id not in self.item_set_countries:
                title, country = self._fetch_item_set_metadata(set_id)
                if title:
                    self.item_set_titles[set_id] = title
                if country:
                    self.item_set_countries[set_id] = country

    def get_items(self, params: Optional[Dict[str, Any]] = None) -> List[OmekaItem]:
        """
        Fetch all items from the Omeka S instance with optional filtering.
        
        This method handles pagination automatically and enriches items with
        additional metadata like resource class labels and item set information.
        
        Args:
            params (Optional[Dict[str, Any]]): Query parameters for filtering items
            
        Returns:
            List[OmekaItem]: List of processed Omeka items
            
        Raises:
            Exception: If item collection fails
        """
        items = []
        logger.info("Starting item collection...")
        
        try:
            for item in self._paginated_fetch(ResourceType.ITEM, params):
                items.append(item)
            
            # After collecting all items, fetch resource class labels and item set metadata
            self._build_resource_class_labels(items)
            self._build_item_set_metadata(items)
            
            # Update items with resource class labels, item set titles, and countries
            for item in items:
                if item.resource_class_id:
                    item.resource_class_label = self.resource_class_labels.get(item.resource_class_id)
                item.item_set_title = self.item_set_titles.get(item.item_set_ids[0]) if item.item_set_ids else None
                for set_id in item.item_set_ids:
                    if set_id in self.item_set_countries:
                        item.country = self.item_set_countries[set_id]
                        break
            
            logger.info(f"Successfully collected {len(items)} items")
            return items
        except Exception as e:
            logger.error(f"Error during item collection: {str(e)}")
            raise

    def get_item_by_id(self, item_id: int) -> Optional[OmekaItem]:
        """Fetch a single item by ID"""
        try:
            logger.info(f"Fetching item {item_id}")
            data = self._make_request(f"items/{item_id}")
            if data:
                item = OmekaItem.from_api_response(data)
                logger.info(f"Successfully fetched item: {item.title}")
                return item
        except ApiError as e:
            logger.error(f"Failed to fetch item {item_id}: {str(e)}")
        return None

    def save_to_cache(self, items: List[OmekaItem], filename: str):
        """
        Save collected items to a cache file.
        
        Args:
            items (List[OmekaItem]): Items to cache
            filename (str): Name of the cache file
            
        Raises:
            IOError: If writing to the cache file fails
        """
        cache_file = self.config.cache_dir / filename
        try:
            logger.info(f"Saving {len(items)} items to cache...")
            items_data = [
                {
                    'id': item.id,
                    'title': item.title,
                    'resource_class_label': item.resource_class_label,
                    'created_date': item.created_date.isoformat(),
                    'publication_date': item.publication_date,
                    'num_pages': item.num_pages,
                    'language': item.language,
                    'word_count': item.word_count,
                    'item_set_title': item.item_set_title,
                    'country': item.country,
                    'type': item.type
                }
                for item in items
            ]
            with cache_file.open('w', encoding='utf-8') as f:
                json.dump(items_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Successfully saved data to {cache_file}")
        except IOError as e:
            logger.error(f"Failed to write cache file {cache_file}: {str(e)}")
            raise

    def load_from_cache(self, filename: str) -> Optional[List[OmekaItem]]:
        """Load items from cache file"""
        cache_file = self.config.cache_dir / filename
        try:
            if cache_file.exists():
                logger.info(f"Loading items from cache: {cache_file}")
                with cache_file.open('r', encoding='utf-8') as f:
                    items_data = json.load(f)
                    items = []
                    for item_data in tqdm(items_data, desc="Loading cached items"):
                        item = OmekaItem(
                            id=item_data['id'],
                            title=item_data['title'],
                            resource_class_label=item_data.get('resource_class_label'),
                            created_date=datetime.fromisoformat(item_data['created_date']),
                            publication_date=item_data['publication_date'],
                            num_pages=item_data['num_pages'],
                            language=item_data['language'],
                            word_count=item_data['word_count'],
                            item_set_title=item_data.get('item_set_title'),
                            country=item_data.get('country'),
                            type=item_data.get('type')
                        )
                        items.append(item)
                logger.info(f"Successfully loaded {len(items)} items from cache")
                return items
        except IOError as e:
            logger.error(f"Failed to read cache file {cache_file}: {str(e)}")
        return None

    def fetch_all_data(self, use_cache: bool = True) -> List[OmekaItem]:
        """Fetch all items with caching support"""
        logger.info("Fetching fresh data from API...")
        items = self.get_items()
        self.save_to_cache(items, 'items.json')
        return items

    def get_item_sets(self) -> List[Dict[str, Any]]:
        """Fetch all item sets"""
        item_sets = list(self._paginated_fetch(ResourceType.ITEM_SET))
        logger.info(f"Fetched {len(item_sets)} item sets")
        return item_sets

    def get_media(self) -> List[Dict[str, Any]]:
        """Fetch all media"""
        media = list(self._paginated_fetch(ResourceType.MEDIA))
        logger.info(f"Fetched {len(media)} media items")
        return media

    def get_items_by_class(self, resource_class_id: int) -> List[Dict[str, Any]]:
        """Fetch items filtered by resource class ID"""
        return self.get_items({'resource_class_id': resource_class_id}) 