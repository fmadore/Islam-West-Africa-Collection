import requests
import plotly.express as px
from collections import defaultdict
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305, 58, 49, 36, 60, 38]  # Extended list of class IDs

def fetch_items_by_class(resource_classes):
    """ Fetch all items that belong to specific resource classes, handling pagination with a progress bar. """
    language_count = defaultdict(int)
    page = 1
    total_fetched = 0
    with tqdm(desc="Fetching items", unit=" items") as pbar:
        while True:
            response = requests.get(f"{api_url}/items", params={"resource_class_id[]": resource_classes, "page": page, "per_page": 50})
            data = response.json()
            if not data:
                break  # Stop if no more data is returned
            num_items = len(data)
            total_fetched += num_items
            pbar.update(num_items)
            for item in data:
                languages = item.get('dcterms:language', [])
                for lang in languages:
                    if 'display_title' in lang:
                        language_count[lang['display_title']] += 1
            page += 1
    return language_count

def create_pie_chart(language_count):
    """ Create a pie chart from the language distribution data. """
    labels = list(language_count.keys())
    values = list(language_count.values())

    fig = px.pie(names=labels, values=values, title='Distribution of Items by Language')
    fig.show()

# Execute the functions
language_distribution = fetch_items_by_class(resource_classes)
create_pie_chart(language_distribution)
