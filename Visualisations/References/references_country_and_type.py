import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
from typing import Dict, List, Any, Tuple, Set

API_URL = "https://islam.zmo.de/api"
ITEM_SET_IDS = [2193, 2212, 2217, 2222, 2225, 2228]
RESOURCE_CLASSES = [35, 43, 88, 40, 82, 178, 52, 77, 305]

FRENCH_LABELS = {
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


def fetch_resource_class_labels() -> Dict[int, Dict[str, str]]:
    labels = {}
    for class_id in RESOURCE_CLASSES:
        response = requests.get(f"{API_URL}/resource_classes/{class_id}")
        if response.status_code == 200:
            class_data = response.json()
            labels[class_id] = {
                'en': class_data.get('o:label', f'Unknown Class {class_id}'),
                'fr': FRENCH_LABELS.get(class_id, f'Classe Inconnue {class_id}')
            }
        else:
            labels[class_id] = {'en': f'Unknown Class {class_id}', 'fr': f'Classe Inconnue {class_id}'}
    return labels


def fetch_items() -> Tuple[List[Dict[str, Any]], int]:
    all_items = []
    unique_item_ids = set()
    for item_set_id in tqdm(ITEM_SET_IDS, desc="Fetching items from all sets"):
        page = 1
        while True:
            response = requests.get(f"{API_URL}/items",
                                    params={"item_set_id": item_set_id, "page": page, "per_page": 50})
            if response.status_code != 200:
                break
            data = response.json()
            if not data:
                break
            for item in data:
                item_id = item['o:id']
                if item_id not in unique_item_ids:
                    unique_item_ids.add(item_id)
                    all_items.append(item)
            page += 1
    return all_items, len(unique_item_ids)


def get_countries_for_item_sets() -> Dict[int, str]:
    countries = {}
    for item_set_id in ITEM_SET_IDS:
        response = requests.get(f"{API_URL}/item_sets/{item_set_id}")
        if response.status_code == 200:
            item_set_data = response.json()
            country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown')
            countries[item_set_id] = country
    return countries


def categorize_items(items: List[Dict[str, Any]], class_labels: Dict[int, Dict[str, str]], countries: Dict[int, str],
                     language: str = 'en') -> Dict[str, Dict[str, int]]:
    items_by_country_and_class = defaultdict(lambda: defaultdict(int))
    for item in items:
        resource_class_id = item.get('o:resource_class', {}).get('o:id')
        item_sets = item.get('o:item_set', [])
        if resource_class_id in class_labels and item_sets:
            label = class_labels[resource_class_id][language]
            item_countries = [countries.get(set_data['o:id'], 'Unknown') for set_data in item_sets]
            for country in item_countries:
                items_by_country_and_class[country][label] += 1
    return items_by_country_and_class


def prepare_data_for_visualization(items_by_country_and_class: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
    return [
        {'Country': country, 'Resource Class': label, 'Number of Items': count}
        for country, classes in items_by_country_and_class.items()
        for label, count in classes.items()
    ]


def create_visualization(data: List[Dict[str, Any]], unique_item_count: int, language: str = 'en') -> px.treemap:
    title = (
        f'Distribution of the {unique_item_count} references in the database by country and type'
        if language == 'en'
        else f'Répartition des {unique_item_count} références dans la base de données par pays et par type'
    )
    note = (
        '<br><sup>Note: A single reference may be related to multiple countries, '
        'resulting in a sum greater than the total unique references.</sup>'
        if language == 'en' else
        '<br><sup>Note : une seule référence peut être liée à plusieurs pays, '
        'ce qui peut donner une somme supérieure au nombre total de références uniques.</sup>'
    )
    fig = px.treemap(data, path=['Country', 'Resource Class'], values='Number of Items', title=title + note)
    fig.update_layout(title_font_size=20)
    fig.update_traces(textinfo="label+value")
    return fig


def save_and_show_visualization(fig: px.treemap, language: str):
    filename = f'treemap_references_type_{language}.html'
    fig.write_html(filename)
    print(f"Graph saved as {filename}")
    fig.show()


def main():
    class_labels = fetch_resource_class_labels()
    countries = get_countries_for_item_sets()
    items, unique_item_count = fetch_items()

    for language in ['en', 'fr']:
        items_by_country_and_class = categorize_items(items, class_labels, countries, language)
        data = prepare_data_for_visualization(items_by_country_and_class)
        fig = create_visualization(data, unique_item_count, language)
        save_and_show_visualization(fig, language)


if __name__ == "__main__":
    main()