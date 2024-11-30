import requests
from collections import defaultdict
from tqdm import tqdm
import os
import logging
from typing import Dict, DefaultDict, List, Optional
from datetime import datetime
import concurrent.futures
from dataclasses import dataclass, field
from pathlib import Path
import json

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Add after SCRIPT_DIR definition
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)  # Create data directory if it doesn't exist

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(SCRIPT_DIR, 'items_over_years.log'),
    filemode='w'
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class to store all constants and settings."""
    API_URL: str = "https://islam.zmo.de/api"
    ITEMS_PER_PAGE: int = 50
    MAX_WORKERS: int = 5
    CACHE_DURATION_HOURS: int = 24
    CACHE_FILE: Path = field(default_factory=lambda: Path(DATA_DIR) / "data_cache.json")
    OUTPUT_FILE: Path = field(default_factory=lambda: Path(DATA_DIR) / "visualization_data.json")
    
    # Resource class IDs and their translations
    ACCEPTABLE_IDS: Dict[int, Dict[str, str]] = field(default_factory=lambda: {
        58: {"en": "Image", "fr": "Image"},
        49: {"en": "Other document", "fr": "Document divers"},
        36: {"en": "Press article", "fr": "Article de presse"},
        60: {"en": "Islamic newspaper", "fr": "Journal islamique"},
        38: {"en": "Audiovisual document", "fr": "Document audiovisuel"},
        35: {"en": "Journal article", "fr": "Article de revue"},
        43: {"en": "Chapter", "fr": "Chapitre"},
        88: {"en": "Thesis", "fr": "Thèse"},
        40: {"en": "Book", "fr": "Livre"},
        82: {"en": "Report", "fr": "Rapport"},
        178: {"en": "Book review", "fr": "Compte rendu de livre"},
        52: {"en": "Edited volume", "fr": "Ouvrage collectif"},
        77: {"en": "Communication", "fr": "Communication"},
        305: {"en": "Blog article", "fr": "Article de blog"}
    })

class DataFetcher:
    """Handles all data fetching operations with caching capability."""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
    
    def _is_cache_valid(self) -> bool:
        """Check if cache exists and is still valid."""
        if not self.config.CACHE_FILE.exists():
            return False
        
        cache_time = datetime.fromtimestamp(self.config.CACHE_FILE.stat().st_mtime)
        age_hours = (datetime.now() - cache_time).total_seconds() / 3600
        return age_hours < self.config.CACHE_DURATION_HOURS

    def _fetch_page(self, page: int) -> Optional[List[dict]]:
        """Fetch a single page of data from the API."""
        try:
            response = self.session.get(
                f"{self.config.API_URL}/items",
                params={'page': page, 'per_page': self.config.ITEMS_PER_PAGE},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return None

    def fetch_items(self, language: str = 'en') -> DefaultDict[str, DefaultDict[str, int]]:
        """Fetch all items with caching support."""
        if self._is_cache_valid():
            logger.info("Using cached data")
            with open(self.config.CACHE_FILE, 'r') as f:
                return defaultdict(lambda: defaultdict(int), json.load(f))

        items_by_year_type: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
        page = 1
        with tqdm(desc="Fetching items", unit=" page") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
                while True:
                    futures = [executor.submit(self._fetch_page, p) for p in range(page, page + self.config.MAX_WORKERS)]
                    results = [f.result() for f in concurrent.futures.as_completed(futures)]
                    
                    if not any(results):
                        break
                    
                    for data in results:
                        if not data:
                            continue
                        self._process_page_data(data, items_by_year_type, language)
                        pbar.update(1)
                    
                    page += self.config.MAX_WORKERS

        # Cache the results
        with open(self.config.CACHE_FILE, 'w') as f:
            json.dump(dict(items_by_year_type), f)

        return items_by_year_type

    def _process_page_data(self, data: List[dict], items_by_year_type: DefaultDict[str, DefaultDict[str, int]], language: str):
        """Process a page of data and update the counts."""
        for item in data:
            resource_classes = item.get('o:resource_class', {})
            if not resource_classes:
                continue

            item_classes = []
            if isinstance(resource_classes, list):
                item_classes = [rclass.get("o:id") for rclass in resource_classes 
                              if rclass.get("o:id") in self.config.ACCEPTABLE_IDS]
            elif isinstance(resource_classes, dict):
                resource_class_id = resource_classes.get("o:id")
                if resource_class_id in self.config.ACCEPTABLE_IDS:
                    item_classes.append(resource_class_id)

            if not item_classes:
                continue

            date_info = item.get('dcterms:date', [])
            if not date_info:
                continue

            date_value = date_info[0].get('@value', '')
            try:
                year = date_value.split('-')[0]
                if not year.isdigit():
                    continue
                for id in item_classes:
                    items_by_year_type[year][self.config.ACCEPTABLE_IDS[id][language]] += 1
            except (IndexError, ValueError) as e:
                logger.warning(f"Error processing date {date_value}: {str(e)}")

def prepare_visualization_data(items_by_year_type: DefaultDict[str, DefaultDict[str, int]], config: Config) -> dict:
    """Prepare data structure for D3.js visualization."""
    # Calculate total counts for each type
    type_totals = defaultdict(int)
    for year_data in items_by_year_type.values():
        for type_name, count in year_data.items():
            type_totals[type_name] += count

    # Calculate yearly totals
    yearly_totals = {}
    for year, types in sorted(items_by_year_type.items()):
        yearly_totals[year] = sum(types.values())

    # Prepare the data structure
    visualization_data = {
        "yearlyData": [],
        "types": [],
        "translations": {
            "types": {},
            "total": {"en": "Total", "fr": "Total"}
        }
    }

    # Add type information
    for type_id, translations in config.ACCEPTABLE_IDS.items():
        en_name = translations["en"]
        if en_name not in visualization_data["types"]:
            visualization_data["types"].append(en_name)
            visualization_data["translations"]["types"][en_name] = {
                "en": translations["en"],
                "fr": translations["fr"]
            }

    # Add yearly data
    for year in sorted(items_by_year_type.keys()):
        year_entry = {
            "year": year,
            "total": yearly_totals[year],
            "values": {}
        }
        for type_name in visualization_data["types"]:
            year_entry["values"][type_name] = items_by_year_type[year].get(type_name, 0)
        
        visualization_data["yearlyData"].append(year_entry)

    return visualization_data

def main():
    """Main execution function."""
    try:
        config = Config()
        data_fetcher = DataFetcher(config)
        
        # Fetch data using English as base language
        items_by_year_type = data_fetcher.fetch_items(language='en')
        
        # Prepare data for visualization
        visualization_data = prepare_visualization_data(items_by_year_type, config)
        
        # Save to JSON file
        with open(config.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(visualization_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Visualization data saved to: {config.OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
