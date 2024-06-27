import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm
import pandas as pd
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
                year = extract_year(item)
                items.append({
                    'id': item_id,
                    'class_id': item.get('o:resource_class', {}).get('o:id'),
                    'year': year
                })
        page += 1
    return items

def extract_year(item: Dict[str, Any]) -> str:
    date_field = item.get('dcterms:date', [{'@value': 'Unknown'}])
    if isinstance(date_field, list) and date_field and '@value' in date_field[0]:
        return date_field[0]['@value'].split('-')[0]
    elif isinstance(date_field, dict):
        return date_field.get('@value', 'Unknown').split('-')[0]
    return 'Unknown'

def fetch_and_categorize_items_by_year(class_labels: Dict[int, Dict[str, str]], language: str = 'en') -> Dict[str, Dict[str, int]]:
    items_by_year_and_class = defaultdict(lambda: defaultdict(int))
    seen_ids = set()
    for item_set_id in tqdm(ITEM_SET_IDS, desc="Processing item sets"):
        items = fetch_items(item_set_id, seen_ids)
        for item in items:
            year = item['year']
            resource_class_id = item['class_id']
            if resource_class_id in class_labels:
                label = class_labels[resource_class_id][language]
                items_by_year_and_class[year][label] += 1
    return items_by_year_and_class

def prepare_data_for_visualization(items_by_year_and_class: Dict[str, Dict[str, int]]) -> pd.DataFrame:
    data = [
        {'Year': year if year.isdigit() else 'Unknown', 'Resource Class': label, 'Number of references': count}
        for year, classes in items_by_year_and_class.items()
        for label, count in classes.items()
    ]
    df = pd.DataFrame(data)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    return df.sort_values(by='Year')

def create_visualization(df: pd.DataFrame, language: str = 'en') -> px.bar:
    total_items = df['Number of references'].sum()
    title = (
        f'Distribution of the {total_items} references in the database over years by type'
        if language == 'en'
        else f'Répartition des {total_items} références de la base de données par année et par type'
    )

    fig = px.bar(
        df,
        x='Year',
        y='Number of references',
        color='Resource Class',
        title=title,
        category_orders={
            "Year": ['Unknown'] + sorted(df['Year'].dropna().unique()),
            "Resource Class": sorted(df['Resource Class'].unique())
        },
        barmode='stack'
    )

    fig.update_layout(
        xaxis_title='Year' if language == 'en' else 'Année',
        yaxis_title='Number of references' if language == 'en' else 'Nombre de références',
        xaxis={'type': 'category'},
        xaxis_tickangle=-45
    )

    return fig

def save_and_show_visualization(fig: px.bar, language: str):
    filename = f'references_distribution_over_years_{language}.html'
    fig.write_html(filename)
    print(f"Graph saved as {filename}")
    fig.show()

def main():
    class_labels = fetch_resource_class_labels()
    for language in ['en', 'fr']:
        items_by_year_and_class = fetch_and_categorize_items_by_year(class_labels, language)
        df = prepare_data_for_visualization(items_by_year_and_class)
        fig = create_visualization(df, language)
        save_and_show_visualization(fig, language)

if __name__ == "__main__":
    main()