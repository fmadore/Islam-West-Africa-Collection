import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"

# Define class IDs and their corresponding names
acceptable_ids = {
    58: "Image",
    49: "Document",
    36: "Article",
    60: "Issue",
    38: "AudioVisualDocument"
}

# Define labels for both English and French
labels = {
    'en': {
        'title': 'Number of Items by Type Over Years',
        'number_of_items': 'Number of Items',
        'year': 'Year',
        'type': 'Item Type',
        'filename': 'item_distribution_over_years_english.html'
    },
    'fr': {
        'title': 'Nombre d\'éléments par type au fil des ans',
        'number_of_items': 'Nombre d\'éléments',
        'year': 'Année',
        'type': 'Type d\'élément',
        'filename': 'item_distribution_over_years_french.html'
    }
}

def fetch_items():
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
                        # Check if resource_classes is a list of dictionaries
                        if isinstance(resource_classes, list):
                            item_classes = [rclass.get("o:id") for rclass in resource_classes if rclass.get("o:id") in acceptable_ids]
                        # Check if resource_classes is a dictionary
                        elif isinstance(resource_classes, dict):
                            resource_class_id = resource_classes.get("o:id")
                            if resource_class_id in acceptable_ids:
                                item_classes.append(resource_class_id)
                        else:
                            # If resource_classes is neither a list nor a dictionary, skip processing it
                            continue

                        # Extract year from date
                        date_info = item.get('dcterms:date', [])
                        if date_info:
                            date_value = date_info[0].get('@value', '')
                            year = date_value.split('-')[0]
                            for id in item_classes:
                                items_by_year_type[year][acceptable_ids[id]] += 1
                page += 1

    return items_by_year_type

def visualize_items_over_years(items_by_year_type, language='en'):
    # Prepare data for visualization
    data = []
    for year, types in sorted(items_by_year_type.items()):  # Ensure data is sorted by year
        for type_name, count in types.items():
            data.append({'Year': year, 'Type': type_name, 'Number of Items': count})

    # Use language-specific labels
    label = labels[language]

    # Create a stacked bar chart
    fig = px.bar(data, x='Year', y='Number of Items', color='Type',
                 title=label['title'],
                 labels={'Number of Items': label['number_of_items'], 'Year': label['year'], 'Type': label['type']},
                 hover_data={'Number of Items': True})
    fig.update_traces(textposition='inside')
    fig.update_layout(
        barmode='stack',
        xaxis={'type': 'category', 'categoryorder': 'category ascending'},
        xaxis_rangeslider_visible=True  # Enable the range slider
    )
    fig.write_html(label['filename'])
    fig.show()

# Fetch data
items_by_year_type = fetch_items()

# Create visualization in English and French
visualize_items_over_years(items_by_year_type, language='en')
visualize_items_over_years(items_by_year_type, language='fr')