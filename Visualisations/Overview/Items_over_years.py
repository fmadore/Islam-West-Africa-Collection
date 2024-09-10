import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
import os

api_url = "https://iwac.frederickmadore.com/api"

# Define class IDs with their corresponding names in both English and French
acceptable_ids = {
    58: {"en": "Image", "fr": "Image"},
    49: {"en": "Other document", "fr": "Document divers"},
    36: {"en": "Press article", "fr": "Article de presse"},
    60: {"en": "Islamic newspaper", "fr": "Journal islamique"},
    38: {"en": "Audiovisual document", "fr": "Document audiovisuel"},
    35: {"en": "Journal article", "fr": "Article de revue"},
    43: {"en": "Chapter", "fr": "Chapitre"},
    88: {"en": "Thesis", "fr": "Thèse"},
    40: {"en": "Book", "fr": "Livre"},
    82: {"en": "Report", "fr": "Rapport"},
    178: {"en": "Book review", "fr": "Compte rendu de livre"},
    52: {"en": "Edited volume", "fr": "Ouvrage collectif"},
    77: {"en": "Communication", "fr": "Communication"},
    305: {"en": "Blog article", "fr": "Article de blog"}
}

# Define labels for both English and French
labels = {
    'en': {
        'title': 'Number of items in the database by type over years',
        'number_of_items': 'Number of items',
        'year': 'Year',
        'type': 'Item type',
        'filename': 'item_distribution_over_years_english.html'
    },
    'fr': {
        'title': 'Nombre d\'éléments de la base de données par type au fil des ans',
        'number_of_items': 'Nombre d\'éléments',
        'year': 'Année',
        'type': 'Type d\'élément',
        'filename': 'item_distribution_over_years_french.html'
    }
}


def fetch_items(language='en'):
    page = 1
    more_pages_available = True
    items_by_year_type = defaultdict(lambda: defaultdict(int))

    with tqdm(desc="Fetching items", unit=" page") as pbar:
        while more_pages_available:
            response = requests.get(f"{api_url}/items", params={'page': page, 'per_page': 50})
            data = response.json()
            if not data:
                more_pages_available = False
            else:
                pbar.update(1)
                for item in data:
                    resource_classes = item.get('o:resource_class', {})
                    if resource_classes:
                        item_classes = []
                        if isinstance(resource_classes, list):
                            item_classes = [rclass.get("o:id") for rclass in resource_classes if
                                            rclass.get("o:id") in acceptable_ids]
                        elif isinstance(resource_classes, dict):
                            resource_class_id = resource_classes.get("o:id")
                            if resource_class_id in acceptable_ids:
                                item_classes.append(resource_class_id)
                        else:
                            continue

                        date_info = item.get('dcterms:date', [])
                        if date_info:
                            date_value = date_info[0].get('@value', '')
                            year = date_value.split('-')[0]
                            for id in item_classes:
                                items_by_year_type[year][acceptable_ids[id][language]] += 1
                page += 1

    return items_by_year_type


def visualize_items_over_years(items_by_year_type, language='en'):
    data = []
    for year, types in sorted(items_by_year_type.items()):
        for type_name, count in types.items():
            data.append({'Year': year, 'Type': type_name, 'Number of Items': count})

    # Sort the data list by 'Type' alphabetically
    data = sorted(data, key=lambda x: x['Type'])

    label = labels[language]

    fig = px.bar(data, x='Year', y='Number of Items', color='Type',
                 title=label['title'],
                 labels={'Number of Items': label['number_of_items'], 'Year': label['year'], 'Type': label['type']},
                 hover_data={'Number of Items': True})
    fig.update_traces(textposition='inside')
    fig.update_layout(barmode='stack', xaxis={'type': 'category', 'categoryorder': 'category ascending'})
    
    # Save the file in the same directory as the script
    fig.write_html(label['filename'])
    fig.show()


# Fetch data for both English and French visualizations
items_by_year_type_en = fetch_items(language='en')
items_by_year_type_fr = fetch_items(language='fr')

# Create visualization in English
visualize_items_over_years(items_by_year_type_en, language='en')

# Create visualization in French
visualize_items_over_years(items_by_year_type_fr, language='fr')
