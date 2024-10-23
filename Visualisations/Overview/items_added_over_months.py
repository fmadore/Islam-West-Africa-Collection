import requests
from collections import defaultdict
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tqdm import tqdm
import os
from datetime import datetime
from typing import Dict, DefaultDict, List, Set
import logging
from dataclasses import dataclass, field
from pathlib import Path
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class to store all constants and settings"""
    API_URL: str = "https://islam.zmo.de/api"
    ITEMS_PER_PAGE: int = 50
    DATE_FORMAT: str = "%Y-%m-%d"
    DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S%z"
    TICK_INTERVAL: int = 30  # Show a tick every 30 days
    
    ACCEPTABLE_IDS: Dict[int, Dict[str, str]] = field(default_factory=lambda: {
        58: {"en": "Image", "fr": "Image"},
        49: {"en": "Other document", "fr": "Document divers"},
        36: {"en": "Press article", "fr": "Article de presse"},
        60: {"en": "Islamic newspaper", "fr": "Journal islamique"},
        38: {"en": "Audiovisual document", "fr": "Document audiovisuel"},
        35: {"en": "References", "fr": "Références"},
        43: {"en": "References", "fr": "Références"},
        88: {"en": "References", "fr": "Références"},
        40: {"en": "References", "fr": "Références"},
        82: {"en": "References", "fr": "Références"},
        178: {"en": "References", "fr": "Références"},
        52: {"en": "References", "fr": "Références"},
        77: {"en": "References", "fr": "Références"},
        305: {"en": "References", "fr": "Références"}
    })
    
    LABELS: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        'en': {
            'title': 'Number of items added to the database by type over months',
            'number_of_items': 'Number of items',
            'month': 'Month',
            'type': 'Item type',
            'filename': 'item_distribution_over_months_english.html'
        },
        'fr': {
            'title': 'Nombre d\'éléments ajoutés à la base de données par type au fil des mois',
            'number_of_items': 'Nombre d\'éléments',
            'month': 'Mois',
            'type': 'Type d\'élément',
            'filename': 'item_distribution_over_months_french.html'
        }
    })

class APIClient:
    """Handle all API-related operations with proper error handling and retries"""
    
    def __init__(self, base_url: str):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.base_url = base_url

    def fetch_items(self, page: int, per_page: int) -> dict:
        """Fetch items from the API with error handling"""
        try:
            response = self.session.get(
                f"{self.base_url}/items",
                params={'page': page, 'per_page': per_page},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return {}

class DataProcessor:
    """Process and organize the data from the API"""
    
    def __init__(self, config: Config, api_client: APIClient):
        self.config = config
        self.api_client = api_client

    def process_item(self, item: dict, language: str) -> Dict[str, int]:
        """Process a single item and return its date and type information"""
        result = defaultdict(int)
        
        resource_classes = item.get('o:resource_class', {})
        if not resource_classes:
            return result

        item_classes = self._extract_item_classes(resource_classes)
        created_date = self._extract_created_date(item)
        
        if created_date and item_classes:
            for id in item_classes:
                result[created_date, self.config.ACCEPTABLE_IDS[id][language]] += 1
                
        return result

    def _extract_item_classes(self, resource_classes) -> List[int]:
        """Extract valid class IDs from resource classes"""
        if isinstance(resource_classes, list):
            return [rclass.get("o:id") for rclass in resource_classes 
                   if rclass.get("o:id") in self.config.ACCEPTABLE_IDS]
        elif isinstance(resource_classes, dict):
            class_id = resource_classes.get("o:id")
            return [class_id] if class_id in self.config.ACCEPTABLE_IDS else []
        return []

    def _extract_created_date(self, item: dict) -> str:
        """Extract and format the creation date from an item"""
        created_date = item.get('o:created', {})
        if isinstance(created_date, dict):
            date_value = created_date.get('@value', '')
            try:
                date_obj = datetime.strptime(date_value, self.config.DATETIME_FORMAT)
                return date_obj.strftime(self.config.DATE_FORMAT)
            except ValueError:
                logger.warning(f"Unable to parse date: {date_value}")
        return ""

    def fetch_all_items(self, language: str) -> DefaultDict[str, DefaultDict[str, int]]:
        """Fetch and process all items from the API"""
        items_by_date_type = defaultdict(lambda: defaultdict(int))
        page = 1
        
        with tqdm(desc=f"Fetching items ({language})", unit=" page") as pbar:
            while True:
                data = self.api_client.fetch_items(page, self.config.ITEMS_PER_PAGE)
                if not data:
                    break
                    
                for item in data:
                    results = self.process_item(item, language)
                    for (date, item_type), count in results.items():
                        items_by_date_type[date][item_type] += count
                
                page += 1
                pbar.update(1)
                
        return items_by_date_type

class Visualizer:
    """Handle the visualization of the processed data"""
    
    def __init__(self, config: Config):
        self.config = config

    def create_visualization(self, items_by_date_type: DefaultDict[str, DefaultDict[str, int]], 
                           language: str) -> None:
        """Create and save the visualization"""
        dates = sorted(items_by_date_type.keys())
        types = sorted(set(type_name for date_types in items_by_date_type.values() 
                         for type_name in date_types))
        
        cumulative_data = self._calculate_cumulative_data(dates, types, items_by_date_type)
        
        fig = self._create_figure(dates, types, cumulative_data, language)
        self._save_figure(fig, language)

    def _calculate_cumulative_data(self, dates: List[str], types: Set[str], 
                                 items_by_date_type: DefaultDict[str, DefaultDict[str, int]]) -> Dict[str, List[int]]:
        """Calculate cumulative data for each type"""
        cumulative_data = {type_name: [0] * len(dates) for type_name in types}
        
        for i, date in enumerate(dates):
            for type_name in types:
                if i > 0:
                    cumulative_data[type_name][i] = cumulative_data[type_name][i-1]
                cumulative_data[type_name][i] += items_by_date_type[date].get(type_name, 0)
                
        return cumulative_data

    def _create_figure(self, dates: List[str], types: Set[str], 
                      cumulative_data: Dict[str, List[int]], language: str) -> go.Figure:
        """Create the Plotly figure"""
        fig = make_subplots(rows=1, cols=1)
        
        for type_name in types:
            fig.add_trace(
                go.Scatter(x=dates, y=cumulative_data[type_name], 
                          name=type_name, mode='lines'),
                row=1, col=1
            )

        label = self.config.LABELS[language]
        fig.update_layout(
            title=label['title'],
            xaxis_title=label['month'],
            yaxis_title=label['number_of_items'],
            legend_title=label['type'],
            hovermode="x unified"
        )

        # Update x-axis to show more labels
        fig.update_xaxes(
            tickformat="%Y-%m",
            tickmode='array',
            tickvals=dates[::self.config.TICK_INTERVAL],
            ticktext=[datetime.strptime(date, self.config.DATE_FORMAT).strftime("%Y-%m") 
                     for date in dates[::self.config.TICK_INTERVAL]],
            tickangle=45,
            nticks=20
        )
        
        return fig

    def _save_figure(self, fig: go.Figure, language: str) -> None:
        """Save the figure to a file in the same folder as the script"""
        script_dir = Path(__file__).parent
        output_path = script_dir / self.config.LABELS[language]['filename']
        fig.write_html(output_path)
        logger.info(f"Saved visualization to {output_path}")
        fig.show()

def main():
    """Main function to orchestrate the data fetching and visualization process"""
    config = Config()
    api_client = APIClient(config.API_URL)
    data_processor = DataProcessor(config, api_client)
    visualizer = Visualizer(config)

    for language in ['en', 'fr']:
        items_by_date_type = data_processor.fetch_all_items(language)
        visualizer.create_visualization(items_by_date_type, language)

if __name__ == "__main__":
    main()
