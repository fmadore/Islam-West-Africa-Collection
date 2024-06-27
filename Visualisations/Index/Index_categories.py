import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio
import plotly.graph_objects as go
from typing import List, Dict, Any

API_URL = "https://iwac.frederickmadore.com/api"
ITEM_SET_IDS = [1, 2, 854, 268, 266]


async def fetch_items_for_item_set(session: aiohttp.ClientSession, item_set_id: int) -> List[Dict[str, Any]]:
    """Fetch all items for a specific item set, handling pagination."""
    items = []
    page = 1
    while True:
        async with session.get(f"{API_URL}/items",
                               params={"item_set_id": item_set_id, "page": page, "per_page": 50}) as response:
            if response.status != 200:
                raise Exception(f"HTTP error {response.status} while fetching items for set {item_set_id}")
            data = await response.json()
            if not data:
                break
            items.extend(data)
            page += 1
    return items


async def fetch_item_set_details(session: aiohttp.ClientSession, item_set_id: int) -> Dict[str, Any]:
    """Fetch details and counts of items for a specific item set."""
    async with session.get(f"{API_URL}/item_sets/{item_set_id}") as response:
        if response.status != 200:
            raise Exception(f"HTTP error {response.status} while fetching set {item_set_id}")
        data = await response.json()
        items = await fetch_items_for_item_set(session, item_set_id)
        return {
            'count': len(items),
            'titles': {d["@language"]: d["@value"] for d in data.get('dcterms:title', [])}
        }


async def fetch_all_item_set_details(item_set_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Fetch details for all item sets concurrently."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_item_set_details(session, item_set_id) for item_set_id in item_set_ids]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching item sets")
        return dict(zip(item_set_ids, results))


def create_bar_chart(item_set_details: Dict[int, Dict[str, Any]], language: str, title: str, x_title: str, y_title: str,
                     filename: str):
    """Create and save a bar chart for the item set details with sorted x-axis."""
    # Create a list of tuples containing (label, count)
    data = [(details['titles'].get(language, "Unknown"), details['count']) for details in item_set_details.values()]

    # Sort the data by count in descending order
    sorted_data = sorted(data, key=lambda x: x[1], reverse=True)

    # Separate the sorted labels and values
    labels, values = zip(*sorted_data)

    total_items = sum(values)

    fig = go.Figure(data=[go.Bar(x=labels, y=values)])
    fig.update_layout(
        title=f"{title} (total: {total_items})",
        xaxis_title=x_title,
        yaxis_title=y_title
    )

    fig.write_html(filename)
    fig.show()


async def main():
    try:
        item_set_details = await fetch_all_item_set_details(ITEM_SET_IDS)

        # Generate and save visualizations in both languages
        create_bar_chart(item_set_details, 'en', 'Number of items in the index by category', 'Categories',
                         'Number of items', 'index_distribution_en.html')
        create_bar_chart(item_set_details, 'fr', 'Nombre d\'éléments dans l\'index par catégories', 'Catégories',
                         'Nombre d\'éléments', 'index_distribution_fr.html')
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())