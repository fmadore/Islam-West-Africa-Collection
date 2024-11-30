import requests
from collections import defaultdict
from tqdm import tqdm
import os
import logging
from typing import Dict, DefaultDict, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
import json
import concurrent.futures

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'items_over_years.log'), 'w'),
        logging.StreamHandler()  # This will show logs in console too
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class to store all constants and settings."""
    API_URL: str = "https://islam.zmo.de/api"
    ITEMS_PER_PAGE: int = 50
    MAX_WORKERS: int = 5
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

    # Add country item sets mapping
    COUNTRY_ITEM_SETS: Dict[str, List[int]] = field(default_factory=lambda: {
        'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502, 
                  2195, 10223, 61062, 60638, 61063, 23452, 2192, 2193, 2194],
        'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 
                        23273, 2197, 2196, 2206, 2198, 2203, 2205, 2204, 2202, 23453, 
                        23448, 23449, 2211, 2212],
        'Côte d\'Ivoire': [23253, 43622, 39797, 45829, 45390, 31882, 57953, 57952, 
                          57951, 57950, 57949, 57948, 57945, 57944, 57943, 62076, 
                          61684, 61320, 61289, 43051, 48249, 15845, 2216, 2217, 63444],
        'Niger': [2223, 2218, 2219, 62021, 2220, 2222],
        'Togo': [9458, 2226, 5499, 5498, 26319, 25304, 26327, 2227, 2228],
        'Nigeria': [2184, 2225]
    })

class DataFetcher:
    """Handles all data fetching operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()

    def _fetch_page(self, page: int) -> Optional[dict]:
        """Fetch a single page of data from the API."""
        try:
            # Get items that belong to any of our defined item sets
            item_set_ids = [id for country_ids in self.config.COUNTRY_ITEM_SETS.values() for id in country_ids]
            item_set_query = ','.join(str(id) for id in item_set_ids)
            
            response = self.session.get(
                f"{self.config.API_URL}/items",
                params={
                    'page': page, 
                    'per_page': self.config.ITEMS_PER_PAGE,
                    'item_set_id[]': item_set_query
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            # Return both the items and metadata
            return {
                'items': data,
                'total_items': int(response.headers.get('Omeka-S-Total-Results', 0))
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return None

    def fetch_items(self, language: str = 'en') -> DefaultDict[str, DefaultDict[str, int]]:
        """Fetch all items using concurrent requests."""
        items_by_year_type: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        logger.info("Fetching first page to determine total items...")
        first_page_data = self._fetch_page(1)
        
        if not first_page_data:
            logger.error("Failed to fetch first page")
            return items_by_year_type

        total_items = first_page_data['total_items']
        if total_items == 0:
            logger.warning("No items found")
            return items_by_year_type

        total_pages = (total_items + self.config.ITEMS_PER_PAGE - 1) // self.config.ITEMS_PER_PAGE
        logger.info(f"Found {total_items} total items across {total_pages} pages")

        # Process first page
        self._process_page_data(first_page_data['items'], items_by_year_type, language)

        # Process remaining pages concurrently
        with tqdm(total=total_pages-1, desc="Fetching pages", unit=" pages") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
                future_to_page = {
                    executor.submit(self._fetch_page, page): page 
                    for page in range(2, total_pages + 1)
                }

                for future in concurrent.futures.as_completed(future_to_page):
                    page = future_to_page[future]
                    try:
                        data = future.result()
                        if data and data['items']:
                            self._process_page_data(data['items'], items_by_year_type, language)
                        else:
                            logger.warning(f"No data returned for page {page}")
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing page {page}: {str(e)}")

        logger.info(f"Processed {total_items} items across {len(items_by_year_type)} years")
        return items_by_year_type

    def _process_page_data(self, data: List[dict], items_by_year_type: DefaultDict[str, DefaultDict[str, int]], language: str):
        """Process a page of data and update the counts."""
        for item in data:
            # Get item sets (countries)
            item_sets = item.get('o:item_set', [])
            item_set_ids = [item_set.get('o:id') for item_set in item_sets if isinstance(item_set, dict)]
            
            # Find matching country
            item_country = None
            for item_set_id in item_set_ids:
                for country, ids in self.config.COUNTRY_ITEM_SETS.items():
                    if item_set_id in ids:
                        item_country = country
                        break
                if item_country:
                    break

            if not item_country:
                continue  # Skip if item doesn't belong to any of our countries

            # Check resource classes
            resource_classes = item.get('o:resource_class', {})
            if not resource_classes:
                continue

            # Get acceptable resource classes
            item_classes = []
            if isinstance(resource_classes, list):
                item_classes = [rclass.get("o:id") for rclass in resource_classes 
                              if rclass.get("o:id") in self.config.ACCEPTABLE_IDS]
            elif isinstance(resource_classes, dict):
                resource_class_id = resource_classes.get("o:id")
                if resource_class_id in self.config.ACCEPTABLE_IDS:
                    item_classes.append(resource_class_id)

            if not item_classes:
                continue  # Skip if no acceptable resource classes

            # Process date
            date_info = item.get('dcterms:date', [])
            if not date_info:
                continue

            date_value = date_info[0].get('@value', '')
            try:
                year = date_value.split('-')[0]
                if not year.isdigit():
                    continue
                for id in item_classes:
                    type_name = self.config.ACCEPTABLE_IDS[id][language]
                    items_by_year_type[year][type_name] += 1
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

    # Add yearly data with country information
    for year in sorted(items_by_year_type.keys()):
        year_entry = {
            "year": year,
            "total": yearly_totals[year],
            "values": {},
            "country_counts": defaultdict(lambda: defaultdict(int))
        }
        
        for key, count in items_by_year_type[year].items():
            if "___" in key:
                # This is a country count
                type_name, country = key.split("___")
                year_entry["country_counts"][type_name][country] = count
            else:
                # This is a regular type count
                year_entry["values"][key] = count
        
        visualization_data["yearlyData"].append(year_entry)

    return visualization_data

def main():
    """Main execution function."""
    try:
        logger.info("Starting data collection process...")
        config = Config()
        data_fetcher = DataFetcher(config)
        
        logger.info("Fetching items...")
        # Fetch data using English as base language
        items_by_year_type = data_fetcher.fetch_items(language='en')
        
        if not items_by_year_type:
            logger.error("No data was collected")
            return

        logger.info("Preparing visualization data...")
        # Prepare data for visualization
        visualization_data = prepare_visualization_data(items_by_year_type, config)
        
        logger.info("Saving data to JSON file...")
        # Save to JSON file
        with open(config.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(visualization_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Visualization data saved to: {config.OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
