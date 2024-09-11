import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
import os

api_url = "https://iwac.frederickmadore.com/api"
session = requests.Session()

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

labels = {
    'en': {
        'title': 'Distribution of items by type and country',
        'type': 'Item type',
        'country': 'Country',
        'count': 'Number of items',
        'filename': 'items_by_type_and_country_sunburst_en.html'
    },
    'fr': {
        'title': 'Répartition des éléments par type et pays',
        'type': 'Type d\'élément',
        'country': 'Pays',
        'count': 'Nombre d\'éléments',
        'filename': 'items_by_type_and_country_sunburst_fr.html'
    }
}

def fetch_item_sets():
    page = 1
    item_sets = []
    while True:
        response = session.get(f"{api_url}/item_sets", params={'page': page, 'per_page': 100})
        data = response.json()
        if not data:
            break
        item_sets.extend(data)
        page += 1
    return item_sets

def create_country_dict(item_sets):
    country_dict = {}
    for item_set in item_sets:
        set_id = item_set['o:id']
        spatial_info = item_set.get('dcterms:spatial', [])
        if spatial_info:
            country = spatial_info[0].get('display_title', 'Unknown Country')
            country_dict[set_id] = country
    return country_dict

def fetch_items():
    page = 1
    items = []
    with tqdm(desc="Fetching items", unit=" page") as pbar:
        while True:
            response = session.get(f"{api_url}/items", params={'page': page, 'per_page': 100})
            data = response.json()
            if not data:
                break
            items.extend(data)
            page += 1
            pbar.update(1)
    return items

def categorize_items(items, country_dict, language):
    items_by_type_and_country = defaultdict(lambda: defaultdict(int))
    for item in items:
        resource_class = item.get('o:resource_class', {})
        if isinstance(resource_class, dict):
            class_id = resource_class.get('o:id')
            if class_id in acceptable_ids:
                item_type = acceptable_ids[class_id][language]
                item_sets = item.get('o:item_set', [])
                country = 'Unknown Country'
                for item_set in item_sets:
                    item_set_id = item_set.get('o:id')
                    if item_set_id in country_dict:
                        country = country_dict[item_set_id]
                        break
                items_by_type_and_country[item_type][country] += 1
    return items_by_type_and_country

def create_sunburst_chart(items_by_type_and_country, language):
    data = []
    for item_type, countries in items_by_type_and_country.items():
        for country, count in countries.items():
            data.append({
                labels[language]['type']: item_type,
                labels[language]['country']: country,
                labels[language]['count']: count
            })

    fig = px.sunburst(
        data,
        path=[labels[language]['type'], labels[language]['country']],
        values=labels[language]['count'],
        title=labels[language]['title']
    )

    output_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(output_dir, labels[language]['filename'])
    fig.write_html(filename)
    fig.show()

# Fetch item sets and create country dictionary
item_sets = fetch_item_sets()
country_dict = create_country_dict(item_sets)

# Fetch items once
items = fetch_items()

# Create visualizations for both languages
for lang in ['en', 'fr']:
    items_by_type_and_country = categorize_items(items, country_dict, lang)
    create_sunburst_chart(items_by_type_and_country, lang)
