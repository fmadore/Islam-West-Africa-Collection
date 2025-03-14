"""Script to export data from an Omeka-S instance to JSON-LD files.

This script connects to an Omeka-S API, fetches items of various types (documents, images,
references, etc.), processes them, and exports them as JSON-LD files. It includes robust
error handling, pagination support, and type safety features.

Environment Variables Required:
    OMEKA_BASE_URL: Base URL of the Omeka-S instance
    OMEKA_KEY_IDENTITY: API key identity
    OMEKA_KEY_CREDENTIAL: API key credential

Usage:
    python JSON_export.py
"""

import os
import json
import logging
from enum import Enum
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Iterator
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ResourceClassId(Enum):
    """Enumeration of Omeka-S resource class IDs.
    
    This enum provides type-safe access to resource class IDs used in the Omeka-S instance.
    Each enum value corresponds to a specific type of resource in the database.
    
    Attributes:
        DOCUMENTS (int): Class ID for general documents
        AUDIO_VISUAL (int): Class ID for audio/visual materials
        IMAGES (int): Class ID for image resources
        And so on...
    """
    DOCUMENTS = 49
    AUDIO_VISUAL = 38
    IMAGES = 58
    INDEX_AUTHORITY = 244
    INDEX_EVENTS = 54
    INDEX_LOCATIONS = 9
    INDEX_ORGANIZATIONS = 96
    INDEX_PERSONS = 94
    ISSUES = 60
    NEWSPAPER_ARTICLES = 36
    REF_ARTICLES = 35
    REF_CHAPTERS = 43
    REF_THESES = 88
    REF_BOOKS = 40
    REF_REPORTS = 82
    REF_REVIEWS = 178
    REF_BOOKS_ALT = 52
    REF_COMMUNICATIONS = 77
    REF_BLOG_POSTS = 305

@dataclass
class Config:
    """Configuration settings for the Omeka-S API connection.
    
    This class manages the configuration settings needed to connect to and interact with
    the Omeka-S API. It includes validation of required environment variables and
    handles path configuration for output files.

    Attributes:
        API_URL (str): Base URL of the Omeka-S instance
        API_KEY_IDENTITY (str): API key identity for authentication
        API_KEY_CREDENTIAL (str): API key credential for authentication
        OUTPUT_DIR (Path): Directory where JSON-LD files will be saved
    """
    API_URL: str = field(default_factory=lambda: os.getenv('OMEKA_BASE_URL', ''))
    API_KEY_IDENTITY: str = field(default_factory=lambda: os.getenv('OMEKA_KEY_IDENTITY', ''))
    API_KEY_CREDENTIAL: str = field(default_factory=lambda: os.getenv('OMEKA_KEY_CREDENTIAL', ''))
    OUTPUT_DIR: Path = field(default_factory=lambda: Path(__file__).parent / 'JSON-LD')

    def __post_init__(self):
        if not all([self.API_URL, self.API_KEY_IDENTITY, self.API_KEY_CREDENTIAL]):
            raise ValueError("Missing required environment variables")
        
        # Ensure API_URL doesn't end with trailing slash
        self.API_URL = self.API_URL.rstrip('/')

class ApiError(Exception):
    """Custom exception for handling API-related errors.
    
    This exception is raised when there are issues with API requests, including
    connection failures, authentication errors, or invalid responses.
    """
    pass

class OmekaApiClient:
    """Client for interacting with the Omeka-S API.
    
    This class handles all direct interactions with the Omeka-S API, including
    authentication, request management, pagination, and error handling.

    Attributes:
        config (Config): Configuration settings for API connection
        session (requests.Session): Persistent session for making API requests
    """

    def __init__(self, config: Config):
        """Initialize the API client with configuration settings.

        Args:
            config (Config): Configuration object containing API credentials and settings
        """
        self.config = config
        self.session = requests.Session()
        self.session.params.update({
            'key_identity': config.API_KEY_IDENTITY,
            'key_credential': config.API_KEY_CREDENTIAL
        })

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Make an HTTP request to the Omeka-S API with retry logic.

        Args:
            endpoint (str): API endpoint to request
            params (Optional[Dict[str, Any]]): Query parameters for the request

        Returns:
            List[Dict[str, Any]]: JSON response from the API

        Raises:
            ApiError: If the request fails after all retry attempts
        """
        url = f"{self.config.API_URL}/{endpoint}"
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
        return []

    def _paginated_fetch(self, endpoint: str, params: Dict[str, Any] = None) -> Iterator[Dict[str, Any]]:
        """Fetch all pages of results from a paginated API endpoint.

        Handles pagination automatically, yielding items from each page of results.

        Args:
            endpoint (str): API endpoint to fetch from
            params (Dict[str, Any], optional): Additional query parameters

        Yields:
            Dict[str, Any]: Individual items from the paginated response
        """
        params = params or {}
        page = 1
        per_page = 100

        while True:
            params.update({'page': page, 'per_page': per_page})
            data = self._make_request(endpoint, params)
            if not data:
                break
            yield from data
            page += 1

    def fetch_items(self, resource_class_id: ResourceClassId) -> List[Dict[str, Any]]:
        """Fetch items of a specific resource class from the Omeka-S API.

        Args:
            resource_class_id (ResourceClassId): The resource class ID to fetch items for

        Returns:
            List[Dict[str, Any]]: List of items matching the resource class ID

        Note:
            Uses tqdm to display a progress bar during fetching
        """
        items = []
        item_type = self.get_item_type_name(resource_class_id)
        logger.info(f"Fetching {item_type}...")

        with tqdm(desc=f"Fetching {item_type}", unit="item") as pbar:
            for item in self._paginated_fetch('items', {'resource_class_id': resource_class_id.value}):
                items.append(item)
                pbar.update(1)

        logger.info(f"Fetched {len(items)} {item_type}")
        return items

    @staticmethod
    def get_item_type_name(resource_class_id: ResourceClassId) -> str:
        """Convert a resource class ID to a human-readable type name.

        Args:
            resource_class_id (ResourceClassId): The resource class ID to get the name for

        Returns:
            str: Human-readable name for the resource class
        """
        item_type_map = {
            ResourceClassId.DOCUMENTS: "documents",
            ResourceClassId.AUDIO_VISUAL: "audio/visual documents",
            ResourceClassId.IMAGES: "images",
            ResourceClassId.INDEX_AUTHORITY: "index items (authority files)",
            ResourceClassId.INDEX_EVENTS: "index items (events)",
            ResourceClassId.INDEX_LOCATIONS: "index items (locations)",
            ResourceClassId.INDEX_ORGANIZATIONS: "index items (organizations)",
            ResourceClassId.INDEX_PERSONS: "index items (persons)",
            ResourceClassId.ISSUES: "issues",
            ResourceClassId.NEWSPAPER_ARTICLES: "newspaper articles",
            ResourceClassId.REF_ARTICLES: "references (articles)",
            ResourceClassId.REF_CHAPTERS: "references (chapters)",
            ResourceClassId.REF_THESES: "references (theses)",
            ResourceClassId.REF_BOOKS: "references (books)",
            ResourceClassId.REF_REPORTS: "references (reports)",
            ResourceClassId.REF_REVIEWS: "references (reviews)",
            ResourceClassId.REF_BOOKS_ALT: "references (books)",
            ResourceClassId.REF_COMMUNICATIONS: "references (communications)",
            ResourceClassId.REF_BLOG_POSTS: "references (blog posts)"
        }
        return item_type_map.get(resource_class_id, f"items (class {resource_class_id.value})")

    def fetch_all_items(self) -> tuple[List[Dict[str, Any]], ...]:
        """Fetch all items from the Omeka-S API across all resource types.

        This method fetches items across multiple categories:
        - Main items (documents, audio/visual, images)
        - Index items (authorities, events, locations, organizations, persons)
        - Issues and newspaper articles
        - References (articles, chapters, theses, etc.)
        - Item sets and media

        Returns:
            tuple: A tuple containing:
                - List of main and index items
                - List of item sets
                - List of media items
                - List of reference items
        """
        logger.info("Starting comprehensive data fetch...")
        
        # Fetch main items
        documents = self.fetch_items(ResourceClassId.DOCUMENTS)
        audio_visual = self.fetch_items(ResourceClassId.AUDIO_VISUAL)
        images = self.fetch_items(ResourceClassId.IMAGES)
        
        # Fetch index items
        index_items = []
        index_classes = [
            ResourceClassId.INDEX_AUTHORITY,
            ResourceClassId.INDEX_EVENTS,
            ResourceClassId.INDEX_LOCATIONS,
            ResourceClassId.INDEX_ORGANIZATIONS,
            ResourceClassId.INDEX_PERSONS
        ]
        for resource_class_id in index_classes:
            index_items.extend(self.fetch_items(resource_class_id))
        
        # Fetch other items
        issues = self.fetch_items(ResourceClassId.ISSUES)
        newspaper_articles = self.fetch_items(ResourceClassId.NEWSPAPER_ARTICLES)
        
        # Fetch sets and media
        item_sets = list(self._paginated_fetch('item_sets'))
        media = list(self._paginated_fetch('media'))
        
        # Fetch references
        references = []
        reference_classes = [
            ResourceClassId.REF_ARTICLES,
            ResourceClassId.REF_CHAPTERS,
            ResourceClassId.REF_THESES,
            ResourceClassId.REF_BOOKS,
            ResourceClassId.REF_REPORTS,
            ResourceClassId.REF_REVIEWS,
            ResourceClassId.REF_BOOKS_ALT,
            ResourceClassId.REF_COMMUNICATIONS,
            ResourceClassId.REF_BLOG_POSTS
        ]
        for resource_class_id in reference_classes:
            references.extend(self.fetch_items(resource_class_id))
        
        logger.info("Completed comprehensive data fetch")
        return (
            documents + audio_visual + images + index_items + issues + newspaper_articles,
            item_sets,
            media,
            references
        )

@dataclass
class ProcessedData:
    """Container for processed Omeka-S items organized by type.
    
    This class stores items fetched from the API, organized into categories based on
    their resource type. Each attribute is a list of items of a specific type.

    Attributes:
        audio_visual_documents (List[Dict[str, Any]]): Audio/visual materials
        documents (List[Dict[str, Any]]): General documents
        images (List[Dict[str, Any]]): Image resources
        And so on...
    """
    audio_visual_documents: List[Dict[str, Any]] = field(default_factory=list)
    documents: List[Dict[str, Any]] = field(default_factory=list)
    images: List[Dict[str, Any]] = field(default_factory=list)
    index: List[Dict[str, Any]] = field(default_factory=list)
    issues: List[Dict[str, Any]] = field(default_factory=list)
    item_sets: List[Dict[str, Any]] = field(default_factory=list)
    media: List[Dict[str, Any]] = field(default_factory=list)
    newspaper_articles: List[Dict[str, Any]] = field(default_factory=list)
    references: List[Dict[str, Any]] = field(default_factory=list)

class JsonLdProcessor:
    """Processes raw API data into organized JSON-LD format.
    
    This class handles the transformation of raw API responses into properly
    structured JSON-LD data, organizing items by type and ensuring proper
    formatting.

    Attributes:
        raw_data (List[Dict[str, Any]]): Unprocessed items from the API
        item_sets (List[Dict[str, Any]]): Item set data
        media (List[Dict[str, Any]]): Media item data
        references (List[Dict[str, Any]]): Reference item data
    """
    def __init__(self, raw_data: List[Dict[str, Any]], item_sets: List[Dict[str, Any]], 
                 media: List[Dict[str, Any]], references: List[Dict[str, Any]]):
        self.raw_data = raw_data
        self.item_sets = item_sets
        self.media = media
        self.references = references

    def process(self) -> ProcessedData:
        """Process raw API data into organized categories.

        This method takes the raw data from the API and organizes it into
        appropriate categories based on item types. It handles the classification
        and sorting of items into their respective collections.

        Returns:
            ProcessedData: Processed and categorized items
        """
        processed_data = ProcessedData()
        processed_data.item_sets = self.item_sets
        processed_data.media = self.media
        processed_data.references = self.references

        for item in tqdm(self.raw_data, desc="Processing items"):
            item_type = self.determine_item_type(item)
            if hasattr(processed_data, item_type):
                getattr(processed_data, item_type).append(item)

        logger.info("Data processing completed")
        return processed_data

    @staticmethod
    def determine_item_type(item: Dict[str, Any]) -> str:
        """Determine the type of an item based on its metadata.

        This method analyzes an item's metadata to determine its type category
        based on both the '@type' field and resource class ID.

        Args:
            item (Dict[str, Any]): The item to analyze

        Returns:
            str: The determined item type category name
        """
        item_types = item.get('@type', [])
        resource_class_id = item.get('o:resource_class', {}).get('o:id')
        
        if 'o:Item' not in item_types:
            return 'other'
            
        type_mapping = {
            'bibo:AudioVisualDocument': 'audio_visual_documents',
            'bibo:Document': 'documents',
            'bibo:Image': 'images'
        }
        
        for item_type, mapped_type in type_mapping.items():
            if item_type in item_types:
                return mapped_type
                
        resource_class_mapping = {
            ResourceClassId.INDEX_AUTHORITY.value: 'index',
            ResourceClassId.INDEX_EVENTS.value: 'index',
            ResourceClassId.INDEX_LOCATIONS.value: 'index',
            ResourceClassId.INDEX_ORGANIZATIONS.value: 'index',
            ResourceClassId.INDEX_PERSONS.value: 'index',
            ResourceClassId.ISSUES.value: 'issues',
            ResourceClassId.NEWSPAPER_ARTICLES.value: 'newspaper_articles'
        }
        
        return resource_class_mapping.get(resource_class_id, 'other')

class JsonLdFileGenerator:
    """Generates JSON-LD files from processed data.
    
    This class handles the final step of writing processed data to disk as
    JSON-LD files, with one file per item type.

    Attributes:
        processed_data (ProcessedData): Organized data ready for export
        output_dir (Path): Directory where files will be written
    """
    def __init__(self, processed_data: ProcessedData, output_dir: Path):
        self.processed_data = processed_data
        self.output_dir = output_dir

    def generate_all_files(self):
        """Generate JSON-LD files for all processed data categories.

        Creates the output directory if it doesn't exist and generates
        a separate JSON-LD file for each non-empty category of items.

        Raises:
            IOError: If there are issues creating the output directory or writing files
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        for field_name in self.processed_data.__dataclass_fields__:
            items = getattr(self.processed_data, field_name)
            if items:
                self.generate_json_file(field_name, items)

    def generate_json_file(self, item_type: str, items: List[Dict[str, Any]]):
        """Generate a JSON-LD file for a specific item type.

        Args:
            item_type (str): The type of items being written
            items (List[Dict[str, Any]]): The items to write to the file

        Raises:
            IOError: If there are issues writing the file
        """
        filepath = self.output_dir / f"{item_type}.json"
        
        try:
            with filepath.open('w', encoding='utf-8') as jsonfile:
                json.dump(items, jsonfile, ensure_ascii=False, indent=2)
            logger.info(f"Generated {filepath}")
        except IOError as e:
            logger.error(f"Failed to write {filepath}: {str(e)}")
            raise

def main():
    """Main entry point for the Omeka-S data export script.

    This function orchestrates the entire export process:
    1. Loads environment variables
    2. Initializes API client
    3. Fetches data from the API
    4. Processes the fetched data
    5. Generates JSON-LD output files

    Raises:
        ValueError: If configuration is invalid
        ApiError: If API requests fail
        Exception: For unexpected errors
    """
    try:
        logger.info("Starting the Omeka data export process...")
        
        # Load environment variables
        load_dotenv(Path(__file__).parent.parent.parent / '.env')
        
        # Initialize configuration
        config = Config()
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        # Create API client and fetch data
        api_client = OmekaApiClient(config)
        raw_data, item_sets, media, references = api_client.fetch_all_items()

        if not any([raw_data, item_sets, media, references]):
            logger.warning("No data fetched from the API. Exiting.")
            return

        # Process and generate files
        processor = JsonLdProcessor(raw_data, item_sets, media, references)
        processed_data = processor.process()

        generator = JsonLdFileGenerator(processed_data, config.OUTPUT_DIR)
        generator.generate_all_files()

        logger.info("Data export completed successfully.")
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise
    except ApiError as e:
        logger.error(f"API error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()