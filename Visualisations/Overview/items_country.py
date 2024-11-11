import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
import concurrent.futures
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import functools
import json
from pathlib import Path
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    base_url: str = "https://islam.zmo.de/api"
    timeout: int = 15
    max_retries: int = 5
    backoff_factor: float = 0.5
    max_workers: int = 12
    items_per_page: int = 100
    cache_dir: Path = Path(__file__).parent / "cache"

@dataclass
class ItemSetResult:
    country: str
    set_title: str
    item_count: int

class APIClient:
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = self._create_session()
        self.config.cache_dir.mkdir(exist_ok=True)
        self.use_cache = True

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=25,
            pool_maxsize=25,
            pool_block=True
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @functools.lru_cache(maxsize=128)
    def _get_cache_path(self, endpoint: str, params: str) -> Path:
        # Create a unique cache file name based on the endpoint and parameters
        params_hash = hashlib.md5(params.encode()).hexdigest()
        return self.config.cache_dir / f"{endpoint}_{params_hash}.json"

    def _get_cached_response(self, cache_path: Path) -> Optional[dict]:
        if not self.use_cache:
            return None
        if cache_path.exists():
            try:
                with cache_path.open('r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        return None

    def _cache_response(self, cache_path: Path, data: dict) -> None:
        try:
            with cache_path.open('w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Cache write error: {e}")

    def fetch_items(self, item_set_id: int) -> List[dict]:
        cache_path = self._get_cache_path("items", f"set_{item_set_id}")
        cached_data = self._get_cached_response(cache_path)
        
        if cached_data is not None:
            return cached_data

        items = []
        page = 1
        while True:
            try:
                time.sleep(0.1)  # 100ms delay
                response = self.session.get(
                    f"{self.config.base_url}/items",
                    params={
                        "item_set_id": item_set_id,
                        "page": page,
                        "per_page": self.config.items_per_page
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                data = response.json()
                if not data:
                    break
                items.extend(data)
                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching items for set {item_set_id}, page {page}: {str(e)}")
                break

        if items:
            self._cache_response(cache_path, items)
        return items

    def fetch_item_set(self, item_set_id: int) -> Optional[dict]:
        cache_path = self._get_cache_path("item_sets", str(item_set_id))
        cached_data = self._get_cached_response(cache_path)
        
        if cached_data is not None:
            return cached_data

        try:
            response = self.session.get(
                f"{self.config.base_url}/item_sets/{item_set_id}",
                timeout=self.config.timeout
            )
            response.raise_for_status()
            data = response.json()
            if data:
                self._cache_response(cache_path, data)
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching item set {item_set_id}: {str(e)}")
            return None

class DataProcessor:
    @staticmethod
    @functools.lru_cache(maxsize=128)
    def get_title_by_language(titles_str: str, language: str) -> str:
        titles = json.loads(titles_str)
        for title in titles:
            if title.get('@language', '') == language:
                return title['@value']
        return titles[0]['@value'] if titles else 'Unknown Set Title'

    def process_item_set(self, client: APIClient, item_set_id: int, language: str) -> Tuple[Optional[ItemSetResult], Optional[Tuple]]:
        try:
            item_set_data = client.fetch_item_set(item_set_id)
            if not item_set_data:
                return None, (item_set_id, "Failed to fetch item set data")

            titles = item_set_data.get('dcterms:title', [])
            # Convert titles to string for caching
            set_title = self.get_title_by_language(json.dumps(titles), language)

            items = client.fetch_items(item_set_id)
            # Note: country will be passed from the main loop instead
            return ItemSetResult(None, set_title, len(items)), None

        except Exception as e:
            logger.error(f"Error processing item set {item_set_id}: {str(e)}")
            return None, (item_set_id, str(e))

class DataVisualizer:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Define country names in both languages
        self.country_names = {
            'en': {
                'Bénin': 'Benin',
                'Burkina Faso': 'Burkina Faso',
                'Côte d\'Ivoire': 'Côte d\'Ivoire',
                'Niger': 'Niger',
                'Togo': 'Togo',
                'Nigeria': 'Nigeria',
                'Nigéria': 'Nigeria'
            },
            'fr': {
                'Bénin': 'Bénin',
                'Burkina Faso': 'Burkina Faso',
                'Côte d\'Ivoire': 'Côte d\'Ivoire',
                'Niger': 'Niger',
                'Togo': 'Togo',
                'Nigeria': 'Nigéria',
                'Nigéria': 'Nigéria'
            }
        }
        
        # Main colors for countries
        self.country_colors = {
            'Burkina Faso': '#4B5BA0',  # Deeper blue
            'Côte d\'Ivoire': '#D03B3B',  # Deeper red
            'Bénin': '#2E7D32',  # Deeper green
            'Benin': '#2E7D32',  # Same green for English variant
            'Togo': '#6A1B9A',  # Deeper purple
            'Niger': '#E65100',  # Deeper orange
            'Nigeria': '#00838F',  # Deeper cyan
            'Nigéria': '#00838F',  # Same color for French variant
        }

    def generate_color_palette(self, base_color: str, n_colors: int) -> List[str]:
        """Generate a palette of harmonious colors based on the main color."""
        import colorsys
        
        # Convert hex to RGB
        h = base_color.lstrip('#')
        rgb = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        
        # Convert RGB to HSV (better for creating variations)
        hsv = colorsys.rgb_to_hsv(rgb[0]/255, rgb[1]/255, rgb[2]/255)
        
        colors = []
        for i in range(n_colors):
            # Slightly adjust hue and saturation while maintaining the base color's character
            hue = (hsv[0] + (i * 0.05)) % 1.0  # Small hue shifts
            saturation = max(0.3, hsv[1] - (i * 0.1))  # Gradually reduce saturation
            value = min(0.95, hsv[2] + (i * 0.05))  # Slightly adjust value
            
            # Convert back to RGB
            rgb = colorsys.hsv_to_rgb(hue, saturation, value)
            
            # Convert to hex
            hex_color = '#{:02x}{:02x}{:02x}'.format(
                int(rgb[0] * 255),
                int(rgb[1] * 255),
                int(rgb[2] * 255)
            )
            colors.append(hex_color)
        
        return colors

    def create_visualization(self, items_by_country_and_set: Dict, language: str = 'en'):
        def format_number(n: int) -> str:
            return f"{n:,}".replace(',', ' ')

        # Add language-specific text
        text_map = {
            'en': {'total': 'Total'},
            'fr': {'total': 'Total'}
        }
        text = text_map.get(language, text_map['en'])

        # Calculate total items first
        total_items = sum(
            count for country_data in items_by_country_and_set.values() 
            for count in country_data.values()
        )

        title_map = {
            'en': f'Distribution of {format_number(total_items)} items by country and sub-collection',
            'fr': f'Répartition des {format_number(total_items)} éléments par pays et sous-collection'
        }
        title = title_map.get(language, title_map['en'])

        # Create flattened data structure with translated country names
        data = []
        for country, sets in items_by_country_and_set.items():
            translated_country = self.country_names[language][country]
            
            country_total = sum(sets.values())
            country_percentage = (country_total / total_items) * 100
            
            sorted_sets = dict(sorted(sets.items(), key=lambda x: x[1], reverse=True))
            n_sets = len(sorted_sets)
            color_palette = self.generate_color_palette(self.country_colors[country], n_sets)
            
            for i, (set_title, count) in enumerate(sorted_sets.items()):
                set_percentage = (count / country_total) * 100
                data.append({
                    'Country': f"<b style='font-size: 16px'>{translated_country}</b>",
                    'Item Set Title': f"<b>{set_title}</b>",
                    'Number of Items': count,
                    'text': f"<b>{set_title}</b><br>{text['total']}: {format_number(count)} items<br>{set_percentage:.1f}%",
                    'color': color_palette[i]
                })

        fig = px.treemap(
            data,
            path=['Country', 'Item Set Title'],
            values='Number of Items',
            title=title,
            custom_data=['text']
        )

        # Update traces for all nodes except root
        fig.update_traces(
            textinfo="label+value+percent parent",
            hovertemplate="%{customdata[0]}<extra></extra>",
            marker_colors=[d['color'] for d in data],
            textfont={"size": 14},
            marker_line=dict(width=1, color='white'),
            opacity=0.85,
            root_color="lightgrey"
        )

        # Simply remove hover for root node
        fig.data[0].texttemplate = ""
        fig.data[0].hovertemplate = None

        fig.update_layout(
            font_family="Arial",
            title={
                'font_size': 24,
                'x': 0.5,
                'xanchor': 'center',
                'y': 0.95,
                'yanchor': 'top'
            },
            margin=dict(t=100, l=25, r=25, b=25),
            paper_bgcolor='rgba(250,250,250,1)',
        )
        
        output_file = os.path.join(self.output_dir, f'item_distribution_by_country_and_set_{language}.html')
        fig.write_html(output_file)
        logger.info(f"Visualization saved to {output_file}")
        return fig

def clear_cache(cache_dir: Path, max_age_days: int = 7) -> None:
    """Clear cache files older than max_age_days."""
    current_time = time.time()
    for cache_file in cache_dir.glob("*.json"):
        if (current_time - cache_file.stat().st_mtime) > (max_age_days * 86400):
            cache_file.unlink()

def prompt_cache_usage() -> bool:
    """Prompt user for cache usage preference."""
    while True:
        response = input("Would you like to use cached data? (y/n): ").lower()
        if response in ['y', 'n']:
            return response == 'y'
        print("Please enter 'y' for yes or 'n' for no.")

def main(languages: List[str]):
    # Define item sets organized by country
    country_item_sets = {
        'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502, 2195, 10223, 61062, 60638, 61063, 23452, 2192, 2193, 2194],
        'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 23273, 2197, 2196, 2206, 2198, 2203, 2205, 2204, 2202, 23453, 23448, 23449, 2211, 2212],
        'Côte d\'Ivoire': [23253, 43622, 39797, 45829, 45390, 31882, 57953, 57952, 57951, 57950, 57949, 57948, 57945, 57944, 57943, 62076, 61684, 61320, 61289, 43051, 48249, 15845, 2216, 2217],
        'Niger': [2223, 2218, 2219, 62021, 2220, 2222],
        'Togo': [9458, 2226, 5499, 5498, 26319, 25304, 26327, 2227, 2228],
        'Nigeria': [2184, 2225]
    }

    config = APIConfig()
    
    # Add cache prompt at the start
    use_cache = prompt_cache_usage()
    if not use_cache:
        clear_cache(config.cache_dir)  # Clear all cache if user doesn't want to use it
    
    client = APIClient(config)
    client.use_cache = use_cache  # Set the cache preference
    
    processor = DataProcessor()
    visualizer = DataVisualizer(os.path.join(os.path.dirname(__file__), '..'))

    for language in languages:
        logger.info(f"Processing data for language: {language}")
        items_by_country_and_set = defaultdict(lambda: defaultdict(int))
        errors = []

        for country, item_set_ids in country_item_sets.items():
            with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                future_to_item_set = {
                    executor.submit(processor.process_item_set, client, item_set_id, language): (item_set_id, country)
                    for item_set_id in item_set_ids
                }
                
                for future in tqdm(
                    concurrent.futures.as_completed(future_to_item_set),
                    total=len(item_set_ids),
                    desc=f"Processing {country} item sets ({language})"
                ):
                    item_set_id, country = future_to_item_set[future]
                    result, error = future.result()
                    if error:
                        errors.append(error)
                    elif result:
                        # Use the country from our dictionary instead of from the API
                        items_by_country_and_set[country][result.set_title] += result.item_count

        if errors:
            logger.warning(f"Errors occurred with {len(errors)} item sets:")
            for item_set_id, error in errors:
                logger.warning(f"Item set {item_set_id}: {error}")

        fig = visualizer.create_visualization(items_by_country_and_set, language)
        fig.show()

if __name__ == "__main__":
    config = APIConfig()
    clear_cache(config.cache_dir)  # Optional: clear old cache files
    main(['en', 'fr'])
