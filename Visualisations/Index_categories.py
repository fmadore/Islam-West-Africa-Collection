import requests
import plotly.graph_objects as go
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
item_set_ids = [1, 2, 854, 268, 266]


def fetch_items_for_item_set(item_set_id):
    """ Fetch all items for a specific item set, handling pagination. """
    items = []
    page = 1
    while True:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        items.extend(data)
        page += 1
    return items


def fetch_item_set_details(item_set_ids):
    """ Fetch details and counts of items for specific item sets, including multilingual titles. """
    item_set_details = {}
    for item_set_id in tqdm(item_set_ids, desc="Fetching item sets"):
        response = requests.get(f"{api_url}/item_sets/{item_set_id}")
        if response.status_code == 200:
            data = response.json()
            items = fetch_items_for_item_set(item_set_id)
            item_set_details[item_set_id] = {
                'count': len(items),
                'titles': {d["@language"]: d["@value"] for d in data.get('dcterms:title', [])}
            }
        else:
            print(f"Failed to fetch data for item set ID {item_set_id}: HTTP {response.status_code}")
    return item_set_details


def create_bar_chart(item_set_details, language):
    """ Create and save a bar chart for the item set details in the specified language. """
    labels = [details['titles'].get(language, "Unknown") for id, details in item_set_details.items()]
    values = [details['count'] for details in item_set_details.values()]

    fig = go.Figure(data=[go.Bar(x=labels, y=values)])
    title = "Number of items in the index by category" if language == 'en' else "Nombre d'éléments dans l'index par catégories"
    fig.update_layout(title=title, xaxis_title="Item Sets", yaxis_title="Number of Items")

    filename = f"index_distribution_{language}.html"
    fig.write_html(filename)
    fig.show()


# Fetch item set details
item_set_details = fetch_item_set_details(item_set_ids)

# Generate and save visualizations in both languages
create_bar_chart(item_set_details, 'en')  # English version
create_bar_chart(item_set_details, 'fr')  # French version
