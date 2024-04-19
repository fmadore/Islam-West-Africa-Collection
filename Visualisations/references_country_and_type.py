import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
item_set_ids = [2193, 2212, 2217, 2222, 2225, 2228]
resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305]  # Resource class IDs to focus on

# French labels provided for resource classes
french_labels = {
    35: 'Article de revue',
    43: 'Chapitre',
    88: 'Thèse',
    40: 'Livre',
    82: 'Rapport',
    178: 'Compte rendu',
    52: 'Ouvrage collectif',
    77: 'Communication',
    305: 'Article de blog'
}


def fetch_resource_class_labels():
    """ Fetch labels for resource classes. """
    labels = {}
    for class_id in resource_classes:
        response = requests.get(f"{api_url}/resource_classes/{class_id}")
        if response.status_code == 200:
            class_data = response.json()
            labels[class_id] = {
                'en': class_data.get('o:label', f'Unknown Class {class_id}'),
                'fr': french_labels.get(class_id, f'Classe Inconnue {class_id}')  # Use the provided French labels
            }
        else:
            labels[class_id] = {'en': f'Unknown Class {class_id}', 'fr': f'Classe Inconnue {class_id}'}
    return labels


def fetch_items(item_set_id, seen_ids):
    """ Fetch all items within a given item set, handling pagination and checking for duplicates. """
    items = []
    page = 1
    while True:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        for item in data:
            item_id = item['o:id']
            if item_id not in seen_ids:
                seen_ids.add(item_id)
                items.append(item)
        page += 1
    return items


def fetch_and_categorize_items(class_labels, language='en'):
    """ Fetch items and categorize them by country and resource class label. """
    items_by_country_and_class = defaultdict(lambda: defaultdict(int))
    seen_ids = set()  # Track unique item IDs across all sets
    for item_set_id in tqdm(item_set_ids, desc="Processing item sets"):
        item_set_response = requests.get(f"{api_url}/item_sets/{item_set_id}")
        item_set_data = item_set_response.json()
        country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown')

        items = fetch_items(item_set_id, seen_ids)
        for item in items:
            resource_class_id = item.get('o:resource_class', {}).get('o:id')
            if resource_class_id in class_labels:
                label = class_labels[resource_class_id][language]
                items_by_country_and_class[country][label] += 1

    return items_by_country_and_class


def visualize_data(items_by_country_and_class, language='en'):
    """ Visualize the distribution of items by country and resource class label. """
    total_items = sum(count for classes in items_by_country_and_class.values() for count in classes.values())  # Calculate total items
    title = f'Distribution of the {total_items} references in the database by country and type' if language == 'en' else f'Répartition des {total_items} références de la base de données par pays et par type'
    filename = f'treemap_references_type_{language}.html'

    data = []
    for country, classes in items_by_country_and_class.items():
        for label, count in classes.items():
            data.append({'Country': country, 'Resource Class': label, 'Number of Items': count})

    fig = px.treemap(data, path=['Country', 'Resource Class'], values='Number of Items', title=title)
    fig.update_traces(textinfo="label+value+percent parent")
    fig.write_html(filename)
    fig.show()


# Fetch resource class labels
class_labels = fetch_resource_class_labels()

# Generate visualizations in both languages
for lang in ['en', 'fr']:
    items_by_country_and_class = fetch_and_categorize_items(class_labels, language=lang)
    visualize_data(items_by_country_and_class, language=lang)
