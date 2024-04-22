import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
import pandas as pd

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
                date_field = item.get('dcterms:date', [{'@value': 'Unknown Year'}])  # Handle date as list
                year = 'Unknown Year'
                if isinstance(date_field, list) and date_field and '@value' in date_field[0]:
                    year = date_field[0]['@value'].split('-')[0]  # Assume the format could be YYYY-MM-DD or just YYYY
                elif isinstance(date_field, dict):
                    year = date_field.get('@value', 'Unknown Year').split('-')[0]
                items.append({
                    'id': item_id,
                    'class_id': item.get('o:resource_class', {}).get('o:id'),
                    'year': year
                })
        page += 1
    return items


def fetch_and_categorize_items_by_year(class_labels, language='en'):
    """ Fetch items and categorize them by year and resource class label. """
    items_by_year_and_class = defaultdict(lambda: defaultdict(int))
    seen_ids = set()  # Track unique item IDs across all sets
    for item_set_id in tqdm(item_set_ids, desc="Processing item sets"):
        items = fetch_items(item_set_id, seen_ids)
        for item in items:
            year = item['year']
            resource_class_id = item['class_id']
            if year != 'Unknown Year' and resource_class_id in class_labels:
                label = class_labels[resource_class_id][language]
                items_by_year_and_class[year][label] += 1
    return items_by_year_and_class


def visualize_data_by_year(items_by_year_and_class, language='en'):
    """ Visualize the distribution of items by year and resource class label in a grouped bar chart, excluding unknown years. """
    data = []
    for year, classes in sorted(items_by_year_and_class.items()):
        for label, count in sorted(classes.items()):  # Sort classes alphabetically within each year
            data.append({'Year': year, 'Resource Class': label, 'Number of Items': count})
    df = pd.DataFrame(data)
    df['Year'] = pd.Categorical(df['Year'], categories=sorted(df['Year'].unique()), ordered=True)

    # Set title based on language
    if language == 'en':
        title = 'Distribution of Items Over Years by Class'
    else:
        title = 'Répartition des articles par année et par catégorie'

    fig = px.bar(df, x='Year', y='Number of Items', color='Resource Class', title=title,
                 category_orders={"Resource Class": sorted(df['Resource Class'].unique())}, barmode='group')
    fig.update_layout(xaxis_title='Year', yaxis_title='Number of Items', xaxis={'type': 'category'})
    fig.show()

# Fetch resource class labels
class_labels = fetch_resource_class_labels()

# Generate visualizations for each language
for language in ['en', 'fr']:
    items_by_year_and_class = fetch_and_categorize_items_by_year(class_labels, language)
    visualize_data_by_year(items_by_year_and_class, language)
