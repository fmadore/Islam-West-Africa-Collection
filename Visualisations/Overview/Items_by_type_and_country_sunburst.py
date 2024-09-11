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

# Add this constant for the reference resource classes
REFERENCE_RESOURCE_CLASSES = [35, 43, 88, 40, 82, 178, 52, 77, 305]

# Add a new constant for media resource classes
MEDIA_RESOURCE_CLASSES = [36, 60]  # Press article and Islamic newspaper

# Update the labels dictionary
labels = {
    'en': {
        'title': 'Distribution of items by category, type, and country',
        'category': 'Category',
        'type': 'Item type',
        'country': 'Country',
        'count': 'Number of items',
        'filename': 'items_by_category_type_and_country_sunburst_en.html',
        'references': 'References',
        'media': 'Media'
    },
    'fr': {
        'title': 'Répartition des éléments par catégorie, type et pays',
        'category': 'Catégorie',
        'type': 'Type d\'élément',
        'country': 'Pays',
        'count': 'Nombre d\'éléments',
        'filename': 'items_by_category_type_and_country_sunburst_fr.html',
        'references': 'Références',
        'media': 'Médias'
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
    items_by_category_type_and_country = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for item in items:
        resource_class = item.get('o:resource_class', {})
        if isinstance(resource_class, dict):
            class_id = resource_class.get('o:id')
            if class_id in acceptable_ids:
                item_type = acceptable_ids[class_id][language]
                if class_id in REFERENCE_RESOURCE_CLASSES:
                    category = labels[language]['references']
                elif class_id in MEDIA_RESOURCE_CLASSES:
                    category = labels[language]['media']
                else:
                    category = item_type
                item_sets = item.get('o:item_set', [])
                country = 'Unknown Country'
                for item_set in item_sets:
                    item_set_id = item_set.get('o:id')
                    if item_set_id in country_dict:
                        country = country_dict[item_set_id]
                        break
                items_by_category_type_and_country[category][item_type][country] += 1
    return items_by_category_type_and_country

def create_sunburst_chart(items_by_category_type_and_country, language):
    data = []
    for category, types in items_by_category_type_and_country.items():
        for item_type, countries in types.items():
            for country, count in countries.items():
                data.append({
                    labels[language]['category']: category,
                    labels[language]['type']: item_type,
                    labels[language]['country']: country,
                    labels[language]['count']: count
                })

    fig = px.sunburst(
        data,
        path=[labels[language]['category'], labels[language]['type'], labels[language]['country']],
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
    items_by_category_type_and_country = categorize_items(items, country_dict, lang)
    create_sunburst_chart(items_by_category_type_and_country, lang)
