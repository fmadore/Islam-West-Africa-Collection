"""Script to export resource templates from an Omeka-S instance.

This script connects to an Omeka-S API and exports all resource templates to individual
JSON files. Resource templates define the metadata structure for different types of items
in the Omeka-S database. The script includes features for robust error handling,
pagination support, and safe file naming.

Environment Variables Required:
    OMEKA_BASE_URL: Base URL of the Omeka-S instance
    OMEKA_KEY_IDENTITY: API key identity
    OMEKA_KEY_CREDENTIAL: API key credential

Output:
    Creates individual JSON files for each resource template in the
    'Resource_templates' directory, with filenames based on template labels.

Usage:
    python Resource_templates_export.py
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Iterator
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from dataclasses import dataclass, field
import re
from urllib.parse import urljoin
from requests.exceptions import RequestException, HTTPError

def setup_logging(script_dir: Path) -> logging.Logger:
    """Set up logging configuration for the script.

    Configures both file and console logging with appropriate formatting
    and log levels. Log files are stored in the script directory.

    Args:
        script_dir (Path): Directory where the script is located and where
                          log files will be stored

    Returns:
        logging.Logger: Configured logger instance

    Note:
        Creates a log file named 'omeka_export.log' in the script directory
        Console output and file logging are both set to INFO level
    """
    log_file = script_dir / 'omeka_export.log'
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Set up file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Set up logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

@dataclass
class Config:
    """Configuration settings for the Omeka-S API connection.
    
    This class manages the configuration settings needed to connect to and interact with
    the Omeka-S API. It includes validation of required environment variables and
    handles path configuration for output files.

    Attributes:
        API_URL (str): Base URL of the Omeka-S API endpoint
        API_KEY_IDENTITY (str): API key identity for authentication
        API_KEY_CREDENTIAL (str): API key credential for authentication
        OUTPUT_DIR (Path): Directory where resource template files will be saved

    Note:
        The API_URL will be automatically adjusted to ensure it ends with '/api'
    """
    API_URL: str = field(default_factory=str)
    API_KEY_IDENTITY: str = field(default_factory=str)
    API_KEY_CREDENTIAL: str = field(default_factory=str)
    OUTPUT_DIR: Path = field(default_factory=lambda: Path(__file__).parent / 'Resource_templates')
    
    @classmethod
    def from_env(cls, script_dir: Path) -> 'Config':
        """Create a Config instance from environment variables.

        Loads configuration from a .env file in the parent directory and validates
        all required variables are present.

        Args:
            script_dir (Path): Directory where the script is located, used to
                             locate the .env file and set output directory

        Returns:
            Config: Initialized configuration object

        Raises:
            ValueError: If any required environment variables are missing
        """
        # Look for .env file in parent directories
        env_path = script_dir.parent.parent / '.env'
        load_dotenv(env_path)
            
        api_url = os.getenv('OMEKA_BASE_URL')
        key_identity = os.getenv('OMEKA_KEY_IDENTITY')
        key_credential = os.getenv('OMEKA_KEY_CREDENTIAL')
        
        if not all([api_url, key_identity, key_credential]):
            raise ValueError("Missing required environment variables")
            
        # Ensure API URL ends with /api
        api_url = api_url.rstrip('/')
        if not api_url.endswith('/api'):
            api_url += '/api'
            
        return cls(
            API_URL=api_url,
            API_KEY_IDENTITY=key_identity,
            API_KEY_CREDENTIAL=key_credential,
            OUTPUT_DIR=script_dir / 'Resource_templates'
        )

class OmekaApiError(Exception):
    """Custom exception for Omeka-S API-related errors.
    
    This exception is raised for any API-related issues including:
    - HTTP errors (non-200 responses)
    - Network connectivity issues
    - Invalid JSON responses
    - Authentication failures
    """
    pass

class OmekaApiClient:
    """Client for interacting with the Omeka-S API.
    
    This class handles all direct interactions with the Omeka-S API, including
    authentication, request management, pagination, and error handling. It provides
    methods to fetch resource templates and handles API-specific error cases.

    Attributes:
        config (Config): Configuration settings for API connection
        logger (logging.Logger): Logger instance for recording operations
        session (requests.Session): Persistent session for making API requests
    """
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize the API client.

        Args:
            config (Config): Configuration object containing API credentials and settings
            logger (logging.Logger): Logger instance for recording operations
        """
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.params.update({
            'key_identity': config.API_KEY_IDENTITY,
            'key_credential': config.API_KEY_CREDENTIAL
        })

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Make a GET request to the Omeka-S API.

        Handles the actual HTTP request to the API, including error handling
        and response processing.

        Args:
            endpoint (str): API endpoint to request
            params (Optional[Dict[str, Any]]): Query parameters for the request

        Returns:
            List[Dict[str, Any]]: JSON response from the API

        Raises:
            OmekaApiError: For any API-related errors (HTTP errors, network issues, etc.)
        """
        endpoint = endpoint.lstrip('/')
        url = urljoin(self.config.API_URL + '/', endpoint)
        
        params = params or {}
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            self.logger.debug(f"Making request to: {response.url}")
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Resource not found: {url}")
                return []
            raise OmekaApiError(f"HTTP error occurred: {e.response.text}") from e
        except RequestException as e:
            raise OmekaApiError(f"Request failed: {str(e)}") from e
        except json.JSONDecodeError as e:
            raise OmekaApiError(f"Failed to parse JSON response: {str(e)}") from e

    def fetch_resource_templates(self, batch_size: int = 100) -> Iterator[Dict[str, Any]]:
        """Fetch all resource templates from the API.

        Retrieves resource templates in batches using pagination. Uses a progress
        bar to show fetch progress.

        Args:
            batch_size (int, optional): Number of templates to fetch per request.
                                      Defaults to 100.

        Yields:
            Dict[str, Any]: Individual resource template data

        Note:
            Uses tqdm to display a progress bar during fetching
        """
        page = 1
        total_fetched = 0
        
        self.logger.info("Starting to fetch resource templates...")
        
        with tqdm(desc="Fetching resource templates", unit="template") as pbar:
            while True:
                data = self._make_request('resource_templates', {
                    'page': page,
                    'per_page': batch_size
                })
                
                if not data:
                    break
                    
                total_fetched += len(data)
                pbar.update(len(data))
                yield from data
                page += 1

        self.logger.info(f"Fetched {total_fetched} resource templates")

class JsonFileGenerator:
    """Generator for creating JSON files from resource templates.
    
    This class handles the process of writing resource templates to individual
    JSON files, including filename sanitization and error handling.

    Attributes:
        output_dir (Path): Directory where JSON files will be written
        logger (logging.Logger): Logger instance for recording operations
    """
    
    def __init__(self, output_dir: Path, logger: logging.Logger):
        """Initialize the JSON file generator.

        Args:
            output_dir (Path): Directory where JSON files will be written
            logger (logging.Logger): Logger instance for recording operations
        """
        self.output_dir = output_dir
        self.logger = logger

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Create a safe filename from a template label.

        Converts a template label into a valid filename by:
        - Replacing invalid characters with underscores
        - Collapsing multiple underscores
        - Removing leading/trailing underscores

        Args:
            filename (str): Original filename to sanitize

        Returns:
            str: Sanitized filename safe for use in the filesystem
        """
        clean_name = re.sub(r'[^\w\-_\. ]', '_', filename)
        clean_name = re.sub(r'_+', '_', clean_name.replace(' ', '_'))
        return clean_name.strip('_')

    def generate_json_file(self, template: Dict[str, Any]) -> None:
        """Generate a single JSON file from a template.

        Creates a JSON file for a single resource template, using the template's
        label as the filename (after sanitization).

        Args:
            template (Dict[str, Any]): Resource template data to write

        Raises:
            OSError: If there are issues creating the directory or writing the file
        """
        template_id = template.get('o:id', 'unknown')
        template_label = template.get('o:label', f'Unknown_{template_id}')
        
        filename = self.sanitize_filename(template_label) + '.json'
        filepath = self.output_dir / filename
        
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_text(
                json.dumps(template, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            self.logger.info(f"Generated {filepath}")
        except OSError as e:
            self.logger.error(f"Failed to write file {filepath}: {str(e)}")
            raise

    def generate_json_files(self, templates: Iterator[Dict[str, Any]]) -> None:
        """Generate JSON files for all templates.

        Creates the output directory if it doesn't exist and processes all templates,
        creating individual JSON files for each. Uses a progress bar to show progress.

        Args:
            templates (Iterator[Dict[str, Any]]): Iterator of resource templates to process

        Note:
            Continues processing remaining templates even if one fails
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        for template in tqdm(templates, desc="Generating JSON files"):
            try:
                self.generate_json_file(template)
            except Exception as e:
                self.logger.error(f"Failed to process template {template.get('o:id', 'unknown')}: {str(e)}")

def main() -> None:
    """Main execution function for the resource template export script.

    This function orchestrates the entire export process:
    1. Sets up logging
    2. Loads configuration from environment
    3. Initializes API client
    4. Fetches resource templates
    5. Generates JSON files

    The function handles all high-level errors and ensures proper error reporting
    before exiting.

    Raises:
        SystemExit: With exit code 1 if any critical errors occur
    """
    # Get script directory
    script_dir = Path(__file__).parent.absolute()
    
    # Set up logging
    logger = setup_logging(script_dir)
    
    try:
        logger.info("Starting the Omeka resource template export process...")
        
        config = Config.from_env(script_dir)
        logger.info(f"Configuration loaded. API URL: {config.API_URL}")

        api_client = OmekaApiClient(config, logger)
        generator = JsonFileGenerator(config.OUTPUT_DIR, logger)
        
        templates = api_client.fetch_resource_templates()
        generator.generate_json_files(templates)

        logger.info("Resource template export completed successfully.")
        
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise SystemExit(1)
    except OmekaApiError as e:
        logger.error(f"API error: {str(e)}")
        raise SystemExit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        raise SystemExit(1)

if __name__ == "__main__":
    main()