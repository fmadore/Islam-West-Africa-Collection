import requests
import plotly.graph_objects as go
from collections import defaultdict, OrderedDict
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
item_set_id = 2193
resource_classes = {
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

def fetch_items(item_set_id):
    """ Fetch all items for a specific item set, handling pagination. """
    items = []
    page = 1
    total_pages = 1  # Dummy initial value to enter the loop
    pbar = tqdm(total=total_pages, desc="Fetching items")

    while page <= total_pages:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        if page == 1:  # Adjust the total page count after fetching the first page
            total_result_header = response.headers.get('Omeka-S-Total-Results', str(len(data)))
            if total_result_header.isdigit():  # Ensure the header is a digit before converting
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
    """ Organize items by year and resource class. """
    items_by_year_and_class = defaultdict(lambda: defaultdict(int))
    for item in items:
        year = item.get('dcterms:date', [{}])[0].get('@value', 'Unknown').split('-')[0]  # Extract year from the date
        if year.isdigit():  # Ensure that year is a valid number
            year = int(year)
            class_id = item.get('o:resource_class', {}).get('o:id')
            if class_id in resource_classes:
                items_by_year_and_class[year][resource_classes[class_id]] += 1
    # Sort the dictionary by year
    return OrderedDict(sorted(items_by_year_and_class.items()))

def create_bar_chart(items_by_year_and_class, language='en'):
    """ Create and display a bar chart for item distribution over years. """
    data = []
    years_with_data = sorted(items_by_year_and_class.keys())  # Get only the years with data

    # Prepare data only for years that have entries
    year_position = {}  # Map each year to a positional index
    for idx, year in enumerate(years_with_data):
        year_position[year] = idx  # Position index for each year

    for year, classes in items_by_year_and_class.items():
        for class_label, count in classes.items():
            if count > 0:  # Only include if there are items for that class in that year
                data.append({'Year': year_position[year], 'Resource Class': class_label, 'Count': count, 'LabelYear': year})

    fig = go.Figure()
    for class_label in resource_classes.values():
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


# Fetch items from the specified item set
items = fetch_items(item_set_id)

# Parse items by year and resource class
items_by_year_and_class = parse_items_by_year_and_class(items, resource_classes)

# Create visualizations in both languages
create_bar_chart(items_by_year_and_class, language='en')
create_bar_chart(items_by_year_and_class, language='fr')
