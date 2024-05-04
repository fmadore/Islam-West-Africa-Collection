import requests
from tqdm import tqdm
from config import BASE_URL

def fetch_items_from_set(item_set_ids):
    """Fetch items from the API based on set IDs.

    Args:
        item_set_ids (list): List of item set IDs to fetch.

    Returns:
        list: List of fetched items.
    """
    items = []
    for set_id in tqdm(item_set_ids, desc="Fetching item sets"):
        page = 1
        while True:
            response = requests.get(f"{BASE_URL}?item_set_id={set_id}&page={page}")
            data = response.json()
            if not data:
                break
            items.extend(data)
            page += 1
    return items
