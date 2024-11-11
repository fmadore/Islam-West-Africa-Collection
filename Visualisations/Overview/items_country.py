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
    max_workers: int = 8
    items_per_page: int = 50

@dataclass
class ItemSetResult:
    country: str
    set_title: str
    item_count: int

class APIClient:
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch_items(self, item_set_id: int) -> List[dict]:
        items = []
        page = 1
        while True:
            try:
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
        return items

    def fetch_item_set(self, item_set_id: int) -> Optional[dict]:
        try:
            response = self.session.get(
                f"{self.config.base_url}/item_sets/{item_set_id}",
                timeout=self.config.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching item set {item_set_id}: {str(e)}")
            return None

class DataProcessor:
    @staticmethod
    def get_title_by_language(titles: List[dict], language: str) -> str:
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
            set_title = self.get_title_by_language(titles, language)
            country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown Country')

            items = client.fetch_items(item_set_id)
            return ItemSetResult(country, set_title, len(items)), None

        except Exception as e:
            logger.error(f"Error processing item set {item_set_id}: {str(e)}")
            return None, (item_set_id, str(e))

class DataVisualizer:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Simplified color scheme - using color families instead
        self.country_colors = {
            'Burkina Faso': '#4B5BA0',  # Deeper blue
            'Côte d\'Ivoire': '#D03B3B',  # Deeper red
            'Bénin': '#2E7D32',  # Deeper green
            'Togo': '#6A1B9A',  # Deeper purple
            'Niger': '#E65100',  # Deeper orange
            'Nigeria': '#00838F',  # Deeper cyan
        }

    def create_visualization(self, items_by_country_and_set: Dict, language: str = 'en'):
        # Add a number formatting helper function
        def format_number(n: int) -> str:
            return f"{n:,}".replace(',', ' ')

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

        # Create flattened data structure
        data = []
        for country, sets in items_by_country_and_set.items():
            country_total = sum(sets.values())
            country_percentage = (country_total / total_items) * 100
            
            # Sort sets by count in descending order
            sorted_sets = dict(sorted(sets.items(), key=lambda x: x[1], reverse=True))
            
            for set_title, count in sorted_sets.items():
                set_percentage = (count / country_total) * 100
                data.append({
                    'Country': country,
                    'Item Set Title': set_title,
                    'Number of Items': count,
                    'text': f"{set_title}<br>{format_number(count)} items ({set_percentage:.1f}% of {country})",
                    'country_text': f"{country}<br>{format_number(country_total)} items ({country_percentage:.1f}% of total)"
                })

        fig = px.treemap(
            data,
            path=['Country', 'Item Set Title'],
            values='Number of Items',
            title=title,
            custom_data=['text', 'country_text']
        )

        fig.update_traces(
            textinfo="label+value+percent parent",
            hovertemplate="""
                %{customdata[1] if '%{parent}' == 'total' else customdata[0]}
                <extra></extra>
            """,
            marker_colors=[self.country_colors.get(d['Country'], '#808080') for d in data],
            textfont={"size": 14},
            marker_line=dict(width=1, color='white'),
            opacity=0.85,
            root_color="lightgrey"
        )

        # Update the root text separately with space-formatted numbers
        fig.data[0].texttemplate = ""  # Hide text for root node
        fig.data[0].hovertemplate = f"Total: %{{value:,.0f}}".replace(',', ' ') + " items<extra></extra>"

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
            treemapcolorway=[self.country_colors[country] for country in self.country_colors],
        )
        
        output_file = os.path.join(self.output_dir, f'item_distribution_by_country_and_set_{language}.html')
        fig.write_html(output_file)
        logger.info(f"Visualization saved to {output_file}")
        return fig

def main(item_set_ids: List[int], languages: List[str]):
    config = APIConfig()
    client = APIClient(config)
    processor = DataProcessor()
    
    # Use a relative path to go up one level from the script's location
    visualizer = DataVisualizer(os.path.join(os.path.dirname(__file__), '..'))

    for language in languages:
        logger.info(f"Processing data for language: {language}")
        items_by_country_and_set = defaultdict(lambda: defaultdict(int))
        errors = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            future_to_item_set = {
                executor.submit(processor.process_item_set, client, item_set_id, language): item_set_id 
                for item_set_id in item_set_ids
            }
            
            for future in tqdm(
                concurrent.futures.as_completed(future_to_item_set),
                total=len(item_set_ids),
                desc=f"Processing item sets ({language})"
            ):
                result, error = future.result()
                if error:
                    errors.append(error)
                elif result:
                    items_by_country_and_set[result.country][result.set_title] += result.item_count

        if errors:
            logger.warning(f"Errors occurred with {len(errors)} item sets:")
            for item_set_id, error in errors:
                logger.warning(f"Item set {item_set_id}: {error}")

        fig = visualizer.create_visualization(items_by_country_and_set, language)
        fig.show()

if __name__ == "__main__":
    item_set_ids = [
        2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 4922,
        5500, 5501, 5502, 10223, 2218, 2219, 2220, 2196, 2197, 2198, 2199, 2200,
        2201, 2202, 2203, 2204, 2205, 2206, 2207, 2209, 2210, 2211, 2212, 2213,
        2214, 2215, 2216, 2217, 23452, 23453, 23273, 5503, 2222, 2223, 2184,
        2225, 23253, 2226, 2227, 2228, 9458, 25304, 26327, 5499, 5498, 26319,
        31882, 15845, 39797, 43622, 45829, 45390, 57953, 57952, 57951, 57950,
        57949, 57948, 57945, 57944, 57943, 48249, 61062, 60638, 62076, 62021,
        61684, 61320, 61289, 61063
    ]
    main(item_set_ids, ['en', 'fr'])
