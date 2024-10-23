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
    """Set up logging configuration with files stored in script directory."""
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
    """Configuration for the Omeka API client."""
    API_URL: str = field(default_factory=str)
    API_KEY_IDENTITY: str = field(default_factory=str)
    API_KEY_CREDENTIAL: str = field(default_factory=str)
    OUTPUT_DIR: Path = field(default_factory=lambda: Path(__file__).parent / 'Resource_templates')
    
    @classmethod
    def from_env(cls, script_dir: Path) -> 'Config':
        """Create a Config instance from environment variables."""
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
    """Custom exception for Omeka API-related errors."""
    pass

class OmekaApiClient:
    """Client for interacting with the Omeka API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
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
        """Make a GET request to the Omeka API."""
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
        """Fetch all resource templates from the API."""
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
    """Generator for creating JSON files from templates."""
    
    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = output_dir
        self.logger = logger

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Create a safe filename from a template label."""
        clean_name = re.sub(r'[^\w\-_\. ]', '_', filename)
        clean_name = re.sub(r'_+', '_', clean_name.replace(' ', '_'))
        return clean_name.strip('_')

    def generate_json_file(self, template: Dict[str, Any]) -> None:
        """Generate a single JSON file from a template."""
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
        """Generate JSON files for all templates."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        for template in tqdm(templates, desc="Generating JSON files"):
            try:
                self.generate_json_file(template)
            except Exception as e:
                self.logger.error(f"Failed to process template {template.get('o:id', 'unknown')}: {str(e)}")

def main() -> None:
    """Main execution function."""
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