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

def create_bar_chart(
    item_set_details: Dict[int, Dict[str, Any]],
    language: str,
    title: str,
    x_title: str,
    y_title: str,
    filename: str
) -> None:
    """Create and save a bar chart for the item set details with sorted x-axis."""
    # Save in the same directory as the script
    output_path = os.path.join(SCRIPT_DIR, filename)

    # Create a list of tuples containing (label, count)
    data = [(details['titles'].get(language, "Unknown"), details['count'])
            for details in item_set_details.values()]

    # Sort the data by count in descending order
    sorted_data = sorted(data, key=lambda x: x[1], reverse=True)
    labels, values = zip(*sorted_data)
    total_items = sum(values)

    # Create figure with custom styling
    fig = go.Figure(data=[
        go.Bar(
            x=labels,
            y=values,
            text=values,
            textposition='auto',
            hovertemplate='%{x}<br>Count: %{y}<extra></extra>'
        )
    ])

    fig.update_layout(
        title={
            'text': f"{title} (total: {total_items})",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        xaxis_title=x_title,
        yaxis_title=y_title,
        template='plotly_white',
        hoverlabel=dict(bgcolor="white"),
        margin=dict(t=100, l=70, r=40, b=70)
    )

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
