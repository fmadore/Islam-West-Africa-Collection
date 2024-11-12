import asyncio
import aiohttp
import logging
import os
from datetime import datetime
import plotly.graph_objects as go
from typing import List, Dict, Any, Optional, Tuple
from tqdm.asyncio import tqdm_asyncio
from aiohttp import ClientTimeout
from asyncio import Semaphore

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'api_fetcher.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
API_URL = "https://islam.zmo.de/api"
ITEM_SET_IDS = [1, 2, 854, 268, 266]
TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RATE_LIMIT = 5  # Maximum concurrent requests
REQUEST_DELAY = 0.1  # Delay between requests in seconds

# Add these new classes near the top of the file, after the constants

class ChartConfig:
    """Configuration class for chart styling and settings."""
    COLORS = {
        'bar_color': '#1f77b4',
        'grid_color': '#E5E5E5',
        'text_color': '#2F2F2F'
    }
    
    FONT = {
        'family': "Arial, sans-serif",
        'size': {
            'normal': 12,
            'small': 10,
            'large': 14
        }
    }
    
    MARGINS = {
        't': 120,  # top
        'l': 70,   # left
        'r': 40,   # right
        'b': 120   # bottom
    }

class DataProcessor:
    """Class for processing and preparing chart data."""
    @staticmethod
    def prepare_chart_data(item_set_details: Dict[int, Dict[str, Any]], language: str) -> Tuple[List, List, List, int]:
        """Prepare data for the chart, returns labels, values, percentages, and total."""
        data = [(details['titles'].get(language, "Unknown"), details['count'])
                for details in item_set_details.values()]
        
        # Sort data by count in descending order
        sorted_data = sorted(data, key=lambda x: x[1], reverse=True)
        labels, values = zip(*sorted_data)
        total_items = sum(values)
        
        # Calculate percentages
        percentages = [f"{(value/total_items)*100:.1f}%" for value in values]
        
        return labels, values, percentages, total_items

class ChartBuilder:
    """Class for building and configuring charts."""
    @staticmethod
    def create_bar_trace(labels: List, values: List, percentages: List) -> go.Bar:
        """Create a bar trace with custom styling."""
        return go.Bar(
            x=labels,
            y=values,
            text=[f"{value}<br>({pct})" for value, pct in zip(values, percentages)],
            textposition='auto',
            hovertemplate=(
                "<b>%{x}</b><br>" +
                "Count: %{y:,}<br>" +
                "Percentage: %{customdata}<br>" +
                "<extra></extra>"
            ),
            customdata=percentages,
            marker_color=ChartConfig.COLORS['bar_color'],
            textfont=dict(color='white')
        )

    @staticmethod
    def configure_layout(title: str, x_title: str, y_title: str, total_items: int) -> Dict:
        """Configure the chart layout with custom styling."""
        return {
            'title': {
                'text': f"{title}<br><span style='font-size: {ChartConfig.FONT['size']['normal']}px;'>"
                       f"Total: {total_items:,} items</span>",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            'xaxis_title': x_title,
            'yaxis_title': y_title,
            'template': 'plotly_white',
            'hoverlabel': dict(
                bgcolor="white",
                font_size=ChartConfig.FONT['size']['large']
            ),
            'margin': ChartConfig.MARGINS,
            'showlegend': False,
            'plot_bgcolor': 'white',
            'xaxis': {
                'tickangle': 45,
                'tickfont': dict(size=ChartConfig.FONT['size']['normal'], 
                               color=ChartConfig.COLORS['text_color']),
                'gridcolor': ChartConfig.COLORS['grid_color']
            },
            'yaxis': {
                'gridcolor': ChartConfig.COLORS['grid_color'],
                'tickfont': dict(size=ChartConfig.FONT['size']['normal'], 
                               color=ChartConfig.COLORS['text_color']),
                'tickformat': ',d'
            },
            'font': {
                'family': ChartConfig.FONT['family'],
                'color': ChartConfig.COLORS['text_color']
            }
        }

class APIError(Exception):
    """Custom exception for API-related errors."""
    pass

async def make_request(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    retries: int = MAX_RETRIES
) -> Dict:
    """Make an API request with retries and error handling."""
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    error_msg = f"HTTP {response.status}: {await response.text()}"
                    logger.error(f"Request failed: {error_msg}")
                    if response.status == 404:  # If page not found, return empty list
                        return []
                    response.raise_for_status()
        except asyncio.TimeoutError:
            logger.warning(f"Timeout on attempt {attempt + 1}/{retries}")
        except Exception as e:
            logger.error(f"Error on attempt {attempt + 1}/{retries}: {str(e)}")
        
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    raise APIError(f"Failed to fetch data after {retries} attempts")

async def fetch_items_for_item_set(
    session: aiohttp.ClientSession,
    item_set_id: int,
    sem: Semaphore
) -> List[Dict[str, Any]]:
    """Fetch all items for a specific item set, handling pagination."""
    items = []
    page = 1
    
    while True:
        try:
            async with sem:  # Move semaphore to just around the request
                data = await make_request(
                    session,
                    f"{API_URL}/items",
                    params={"item_set_id": item_set_id, "page": page, "per_page": 50}
                )
                
                if not data:  # If we get an empty response, we've reached the end
                    break
                    
                items.extend(data)
                logger.info(f"Retrieved page {page} for set {item_set_id} with {len(data)} items")
                page += 1
                await asyncio.sleep(REQUEST_DELAY)
                
        except Exception as e:
            logger.error(f"Error fetching items for set {item_set_id}, page {page}: {str(e)}")
            break  # Break the loop if we encounter an error
    
    logger.info(f"Retrieved total of {len(items)} items for set {item_set_id}")
    return items

async def fetch_item_set_details(
    session: aiohttp.ClientSession,
    item_set_id: int,
    sem: Semaphore
) -> Dict[str, Any]:
    """Fetch details and counts of items for a specific item set."""
    try:
        async with sem:
            data = await make_request(session, f"{API_URL}/item_sets/{item_set_id}")
        
        # Fetch items after getting the basic details
        items = await fetch_items_for_item_set(session, item_set_id, sem)
        
        return {
            'count': len(items),
            'titles': {d["@language"]: d["@value"] for d in data.get('dcterms:title', [])},
            'items': items
        }
    except Exception as e:
        logger.error(f"Error fetching details for set {item_set_id}: {str(e)}")
        # Return a default structure instead of raising
        return {
            'count': 0,
            'titles': {'en': f'Set {item_set_id} (error)', 'fr': f'Set {item_set_id} (erreur)'},
            'items': []
        }

async def fetch_all_item_set_details(item_set_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Fetch details for all item sets concurrently."""
    timeout = ClientTimeout(total=TIMEOUT_SECONDS)
    sem = Semaphore(RATE_LIMIT)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for item_set_id in item_set_ids:
            task = fetch_item_set_details(session, item_set_id, sem)
            tasks.append(task)
        
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching item sets")
        return dict(zip(item_set_ids, results))

# Update the create_bar_chart function to use the new classes
def create_bar_chart(
    item_set_details: Dict[int, Dict[str, Any]],
    language: str,
    title: str,
    x_title: str,
    y_title: str,
    filename: str
) -> None:
    """Create and save a bar chart for the item set details with sorted x-axis."""
    output_path = os.path.join(SCRIPT_DIR, filename)

    # Process data
    labels, values, percentages, total_items = DataProcessor.prepare_chart_data(item_set_details, language)

    # Create figure
    fig = go.Figure(data=[ChartBuilder.create_bar_trace(labels, values, percentages)])
    
    # Configure layout
    fig.update_layout(ChartBuilder.configure_layout(title, x_title, y_title, total_items))

    # Save the figure
    fig.write_html(output_path)
    logger.info(f"Chart saved to {output_path}")
    fig.show()

async def main():
    start_time = datetime.now()
    logger.info(f"Script started at {start_time}")
    
    try:
        item_set_details = await fetch_all_item_set_details(ITEM_SET_IDS)
        
        # Generate and save visualizations in both languages
        for lang, config in [
            ('en', {
                'title': 'Number of items in the index by category',
                'x_title': 'Categories',
                'y_title': 'Number of items',
                'filename': 'index_distribution_en.html'
            }),
            ('fr', {
                'title': 'Nombre d\'éléments dans l\'index par catégories',
                'x_title': 'Catégories',
                'y_title': 'Nombre d\'éléments',
                'filename': 'index_distribution_fr.html'
            })
        ]:
            create_bar_chart(
                item_set_details,
                lang,
                config['title'],
                config['x_title'],
                config['y_title'],
                config['filename']
            )
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Script completed at {end_time} (Duration: {duration})")

if __name__ == "__main__":
    asyncio.run(main())
