import requests
import plotly.graph_objects as go
from collections import defaultdict, OrderedDict
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
item_set_id = 2193
resource_classes = {
    35: {'en': 'Journal article', 'fr': 'Article de revue'},
    43: {'en': 'Chapter', 'fr': 'Chapitre'},
    88: {'en': 'Thesis', 'fr': 'Thèse'},
    40: {'en': 'Book', 'fr': 'Livre'},
    82: {'en': 'Report', 'fr': 'Rapport'},
    178: {'en': 'Book review', 'fr': 'Compte rendu de livre'},
    52: {'en': 'Edited volume', 'fr': 'Ouvrage collectif'},
    77: {'en': 'Communication', 'fr': 'Communication'},
    305: {'en': 'Blog article', 'fr': 'Article de blog'}
}

def fetch_items(item_set_id):
    items = []
    page = 1
    total_pages = 1
    pbar = tqdm(total=total_pages, desc="Fetching items")
    while page <= total_pages:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        if page == 1:
            total_result_header = response.headers.get('Omeka-S-Total-Results', str(len(data)))
            if total_result_header.isdigit():
                total_pages = int(total_result_header) // 50 + 1
            else:
                total_pages = (len(data) // 50) + 1
            pbar.total = total_pages
            pbar.update()
        items.extend(data)
        page += 1
        pbar.update()
    pbar.close()
    return items


def parse_items_by_year_and_class(items, resource_classes):
    items_by_year_and_class = defaultdict(lambda: defaultdict(int))
    for item in items:
        year = item.get('dcterms:date', [{}])[0].get('@value', 'Unknown').split('-')[0]
        if year.isdigit():
            year = int(year)
            class_id = item.get('o:resource_class', {}).get('o:id')
            if class_id in resource_classes:
                items_by_year_and_class[year][class_id] += 1
    return OrderedDict(sorted(items_by_year_and_class.items()))

def create_bar_chart(items_by_year_and_class, language='en'):
    data = []
    years_with_data = sorted(items_by_year_and_class.keys())
    year_position = {year: idx for idx, year in enumerate(years_with_data)}
    for year, classes in items_by_year_and_class.items():
        for class_id, count in classes.items():
            if count > 0:
                class_label = resource_classes[class_id][language]
                data.append({'Year': year_position[year], 'Resource Class': class_label, 'Count': count, 'LabelYear': year})

    fig = go.Figure()
    for class_id in resource_classes:
        class_label = resource_classes[class_id][language]
        class_data = [x['Count'] for x in data if x['Resource Class'] == class_label]
        class_years = [x['Year'] for x in data if x['Resource Class'] == class_label]
        fig.add_trace(go.Bar(x=class_years, y=class_data, name=class_label))

    title = 'Distribution of Publications by Year and Type' if language == 'en' else 'Répartition des publications par année et type'
    fig.update_layout(
        title=title,
        xaxis_title='Year',
        yaxis_title='Count of Publications',
        barmode='stack',
        xaxis=dict(
            tickmode='array',
            tickvals=list(year_position.values()),
            ticktext=[str(year) for year in years_with_data]
        )
    )
    fig.show()


items = fetch_items(item_set_id)
items_by_year_and_class = parse_items_by_year_and_class(items, resource_classes)
create_bar_chart(items_by_year_and_class, language='en')
create_bar_chart(items_by_year_and_class, language='fr')
