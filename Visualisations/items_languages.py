import requests
import plotly.express as px
from collections import defaultdict
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305, 58, 49, 36, 60, 38]  # Resource class IDs

def fetch_items_by_class(resource_classes):
    """ Fetch all items for specified classes, extracting language info from dcterms:language. """
    language_count = defaultdict(int)
    page = 1
    total_items = 0  # You can use this to show a rough estimate in the progress bar, if API supports total count
    with tqdm(desc="Fetching items", unit=" items") as pbar:
        while True:
            response = requests.get(f"{api_url}/items", params={"resource_class_id[]": resource_classes, "page": page, "per_page": 50})
            data = response.json()
            if not data:
                break
            for item in data:
                languages = item.get('dcterms:language', [])
                for lang in languages:
                    language_label = lang['display_title']  # Assuming this contains the proper language label
                    language_count[language_label] += 1
            page += 1
            pbar.update(len(data))
    return language_count

def create_pie_chart(language_count, title='Distribution of items by language'):
    """ Create a pie chart from the language distribution data. """
    labels = list(language_count.keys())
    values = list(language_count.values())

    fig = px.pie(names=labels, values=values, title=title)
    fig.show()

# Fetch and visualize data
language_distribution = fetch_items_by_class(resource_classes)
create_pie_chart(language_distribution, title='Distribution of items by language')  # English version

# Assuming French translation for the chart title is needed
create_pie_chart(language_distribution, title='Répartition des éléments par langue')  # French version
