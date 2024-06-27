import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
from typing import Dict, List, Any

API_URL = "https://iwac.frederickmadore.com/api"
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

def fetch_items(item_set_id: int, seen_ids: set) -> List[Dict[str, Any]]:
    items = []
    page = 1
    while True:
        response = requests.get(f"{API_URL}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        if response.status_code != 200:
            break
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

def get_country_from_item_set(item_set_id: int) -> str:
    response = requests.get(f"{API_URL}/item_sets/{item_set_id}")
    if response.status_code == 200:
        item_set_data = response.json()
        return item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown')
    return 'Unknown'

def fetch_and_categorize_items(class_labels: Dict[int, Dict[str, str]], language: str = 'en') -> Dict[str, Dict[str, int]]:
    items_by_country_and_class = defaultdict(lambda: defaultdict(int))
    seen_ids = set()
    for item_set_id in tqdm(ITEM_SET_IDS, desc="Processing item sets"):
        country = get_country_from_item_set(item_set_id)
        items = fetch_items(item_set_id, seen_ids)
        for item in items:
            resource_class_id = item.get('o:resource_class', {}).get('o:id')
            if resource_class_id in class_labels:
                label = class_labels[resource_class_id][language]
                items_by_country_and_class[country][label] += 1
    return items_by_country_and_class

def prepare_data_for_visualization(items_by_country_and_class: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
    return [
        {'Country': country, 'Resource Class': label, 'Number of Items': count}
        for country, classes in items_by_country_and_class.items()
        for label, count in classes.items()
    ]

def create_visualization(data: List[Dict[str, Any]], total_items: int, language: str = 'en') -> px.treemap:
    title = (
        f'Distribution of the {total_items} references in the database by country and type'
        if language == 'en'
        else f'Répartition des {total_items} références de la base de données par pays et par type'
    )
    fig = px.treemap(data, path=['Country', 'Resource Class'], values='Number of Items', title=title)
    fig.update_traces(textinfo="label+value+percent parent")
    return fig

def save_and_show_visualization(fig: px.treemap, language: str):
    filename = f'treemap_references_type_{language}.html'
    fig.write_html(filename)
    print(f"Graph saved as {filename}")
    fig.show()

def main():
    class_labels = fetch_resource_class_labels()
    for language in ['en', 'fr']:
        items_by_country_and_class = fetch_and_categorize_items(class_labels, language)
        data = prepare_data_for_visualization(items_by_country_and_class)
        total_items = sum(item['Number of Items'] for item in data)
        fig = create_visualization(data, total_items, language)
        save_and_show_visualization(fig, language)

if __name__ == "__main__":
    main()