import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
item_set_ids = [2193, 2212, 2217, 2222, 2225, 2228]
resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305]  # Resource class IDs to focus on

def fetch_items(item_set_id):
    """ Fetch all items within a given item set, handling pagination. """
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

def fetch_and_categorize_items():
    """ Fetch items and categorize them by country and resource class. """
    items_by_country_and_class = defaultdict(lambda: defaultdict(int))
    for item_set_id in tqdm(item_set_ids, desc="Processing item sets"):
        item_set_response = requests.get(f"{api_url}/item_sets/{item_set_id}")
        item_set_data = item_set_response.json()
        country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown')

        items = fetch_items(item_set_id)
        for item in items:
            resource_class_id = item.get('o:resource_class', {}).get('o:id')
            if resource_class_id in resource_classes:
                items_by_country_and_class[country][resource_class_id] += 1

    return items_by_country_and_class

def visualize_data(items_by_country_and_class):
    """ Visualize the distribution of items by country and resource class. """
    data = []
    for country, classes in items_by_country_and_class.items():
        for class_id, count in classes.items():
            data.append({'Country': country, 'Resource Class ID': str(class_id), 'Number of Items': count})

    fig = px.treemap(data, path=['Country', 'Resource Class ID'], values='Number of Items',
                     title='Items Distribution by Country and Resource Class')
    fig.update_traces(textinfo="label+value+percent parent")
    fig.write_html('treemap_distribution_by_class.html')
    fig.show()

# Run the processing and visualization functions
items_by_country_and_class = fetch_and_categorize_items()
visualize_data(items_by_country_and_class)
