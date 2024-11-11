import requests
import plotly.express as px
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import time

# Get the directory where the script is located
SCRIPT_DIR = Path(__file__).parent

# Set up logging in the script directory
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / 'omeka_script.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class OmekaConfig:
    """Configuration class for Omeka API settings."""
    base_url: str
    key_identity: str
    key_credential: str
    item_sets: Dict[str, List[int]]

class OmekaAPIClient:
    """Client for interacting with Omeka API."""
    
    def __init__(self, config: OmekaConfig):
        self.config = config
        self.session = self._setup_session()
        self.last_request_time = 0
        self.min_request_interval = 0.1  # Minimum time between requests in seconds

    @staticmethod
    def _setup_session() -> requests.Session:
        """Set up a requests session with retry strategy and increased pool size."""
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=25,
            pool_maxsize=25,
            pool_block=False
        )
        
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()

    def get_item_set_name(self, item_set_id: int) -> str:
        """Fetch item set name using its ID."""
        self._rate_limit()
        url = f"{self.config.base_url}/item_sets/{item_set_id}"
        params = {
            'key_identity': self.config.key_identity,
            'key_credential': self.config.key_credential
        }
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get('dcterms:title', [{'@value': f'Newspaper {item_set_id}'}])[0]['@value']
        except Exception as e:
            logger.error(f"Failed to fetch item set name for ID {item_set_id}: {e}")
            return f"Newspaper {item_set_id}"

    def fetch_items_page(self, item_set_id: int, page: int) -> List[dict]:
        """Fetch items for a specific page of an item set."""
        self._rate_limit()
        url = f"{self.config.base_url}/items"
        params = {
            'key_identity': self.config.key_identity,
            'key_credential': self.config.key_credential,
            'item_set_id': item_set_id,
            'page': page,
            'per_page': 100
        }
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch page {page} for set {item_set_id}: {e}")
            return []

    def get_items_by_set(self, item_set_id: int) -> List[dict]:
        """Retrieve all items for a specific item set ID using pagination."""
        items = []
        page = 1
        while True:
            response = self.fetch_items_page(item_set_id, page)
            if not response:
                break
            items.extend(response)
            page += 1
            logger.debug(f"Retrieved page {page} for set {item_set_id} with {len(response)} items")
        return items

class ContentProcessor:
    """Process and analyze content from Omeka items."""
    
    @staticmethod
    def extract_content(items: List[dict], country: str, newspaper: str) -> List[dict]:
        """Extract content and compute word counts."""
        content_data = []
        for item in items:
            for value in item.get('bibo:content', []):
                if value.get('type') == 'literal' and '@value' in value:
                    word_count = len(value['@value'].split())
                    content_data.append({
                        'country': country,
                        'newspaper': newspaper,
                        'word_count': word_count
                    })
        return content_data

    @staticmethod
    def format_number(number: int) -> str:
        """Format number with spaces as thousand separators."""
        return f"{number:,}".replace(",", " ")

    @staticmethod
    def create_label(newspaper: str, word_count: int, language: str = 'en') -> str:
        """Create a formatted label for visualizations."""
        formatted_count = ContentProcessor.format_number(word_count)
        return f"{newspaper}<br>{formatted_count} {'mots' if language == 'fr' else 'words'}"

class DataVisualizer:
    """Generate and save visualizations."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    def create_treemap(self, df: pd.DataFrame, language: str = 'en') -> None:
        """Create and save treemap visualization."""
        total_words = ContentProcessor.format_number(df['word_count'].sum())
        
        title = {
            'en': f'Total word count: {total_words} - distribution by country and newspaper',
            'fr': f'Nombre total de mots: {total_words} - répartition par pays et journal'
        }[language]

        df['label'] = df.apply(
            lambda x: ContentProcessor.create_label(x['newspaper'], x['word_count'], language),
            axis=1
        )

        fig = px.treemap(
            df,
            path=['country', 'label'],
            values='word_count',
            title=title
        )

        # Show the figure
        fig.show()

        # Save the figure
        output_file = self.output_dir / f"treemap_word_count_{language}.html"
        fig.write_html(str(output_file))
        logger.info(f"Saved treemap visualization to {output_file}")

def main():
    # Load environment variables
    env_path = SCRIPT_DIR.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    # Initialize configuration
    config = OmekaConfig(
        base_url=os.getenv("OMEKA_BASE_URL"),
        key_identity=os.getenv("OMEKA_KEY_IDENTITY"),
        key_credential=os.getenv("OMEKA_KEY_CREDENTIAL"),
        item_sets={
            'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502, 2195, 10223, 61062, 60638, 61063],
            'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 23273, 2197, 2196, 2206, 2198, 2203, 2205, 2204, 2202],
            'Côte d\'Ivoire': [23253, 43622, 39797, 45829, 45390, 31882, 57953, 57952, 57951, 57950, 57949, 57948, 57945, 57944, 57943, 62076, 61684, 61320, 61289],
            'Niger': [2223, 2218, 2219],
            'Togo': [9458, 2226, 5499, 5498, 26319]
        }
    )

    # Initialize clients and processors
    api_client = OmekaAPIClient(config)
    content_processor = ContentProcessor()
    visualizer = DataVisualizer(SCRIPT_DIR)

    # Collect data
    all_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_set = {
            executor.submit(api_client.get_item_set_name, set_id): (country, set_id)
            for country, sets in config.item_sets.items()
            for set_id in sets
        }

        total_sets = len(future_to_set)
        completed = 0
        
        for future in as_completed(future_to_set):
            country, set_id = future_to_set[future]
            try:
                newspaper = future.result()
                items = api_client.get_items_by_set(set_id)
                content_data = content_processor.extract_content(items, country, newspaper)
                all_data.extend(content_data)
                completed += 1
                logger.info(f"Processed {completed}/{total_sets}: {newspaper} ({country}) - {len(items)} items")
            except Exception as e:
                logger.error(f"Error processing {country} - {set_id}: {e}")

    # Create DataFrame and generate visualizations
    df = pd.DataFrame(all_data)
    aggregated_data = df.groupby(['country', 'newspaper'])['word_count'].sum().reset_index()

    # Generate visualizations in both languages
    for lang in ['en', 'fr']:
        visualizer.create_treemap(aggregated_data, lang)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)