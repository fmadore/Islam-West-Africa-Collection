import requests
from collections import defaultdict
import plotly.express as px
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
    CACHE_FILE: Path = field(default_factory=lambda: Path(SCRIPT_DIR) / "data_cache.json")
    
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

    # UI Labels
    LABELS: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        'en': {
            'title': 'Number of items in the database by type over years',
            'number_of_items': 'Number of items',
            'year': 'Year',
            'type': 'Item type',
            'filename': 'item_distribution_over_years_english.html'
        },
        'fr': {
            'title': 'Nombre d\'éléments de la base de données par type au fil des ans',
            'number_of_items': 'Nombre d\'éléments',
            'year': 'Année',
            'type': 'Type d\'élément',
            'filename': 'item_distribution_over_years_french.html'
        }
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

class Visualizer:
    """Handles all visualization operations."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def create_visualization(self, items_by_year_type: DefaultDict[str, DefaultDict[str, int]], language: str = 'en'):
        """Create and save the visualization."""
        data = []
        for year, types in sorted(items_by_year_type.items()):
            for type_name, count in types.items():
                data.append({'Year': year, 'Type': type_name, 'Number of Items': count})

        data = sorted(data, key=lambda x: x['Type'])
        label = self.config.LABELS[language]

        fig = px.bar(
            data,
            x='Year',
            y='Number of Items',
            color='Type',
            title=label['title'],
            labels={
                'Number of Items': label['number_of_items'],
                'Year': label['year'],
                'Type': label['type']
            },
            hover_data={'Number of Items': True}
        )
        
        fig.update_traces(textposition='inside')
        fig.update_layout(
            barmode='stack',
            xaxis={'type': 'category', 'categoryorder': 'category ascending'},
            showlegend=True,
            legend={'title': label['type']},
        )

        # Create the full path for the output file
        output_path = os.path.join(SCRIPT_DIR, label['filename'])
        
        fig.write_html(output_path)
        logger.info(f"Visualization saved to: {output_path}")
        fig.show()

def main():
    """Main execution function."""
    try:
        config = Config()
        data_fetcher = DataFetcher(config)
        visualizer = Visualizer(config)

        for language in ['en', 'fr']:
            logger.info(f"Processing {language} visualization...")
            items_by_year_type = data_fetcher.fetch_items(language=language)
            visualizer.create_visualization(items_by_year_type, language=language)
            logger.info(f"Completed {language} visualization")

        logger.info(f"Data cache saved to: {config.CACHE_FILE}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
