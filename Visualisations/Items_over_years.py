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
                    resource_classes = item.get('o:resource_class')
                    if resource_classes:
                        # Extract class IDs
                        if isinstance(resource_classes, list):
                            item_classes = [rclass.get("o:id") for rclass in resource_classes if
                                            rclass.get("o:id") in acceptable_ids]
                        elif isinstance(resource_classes, dict):
                            item_classes = [resource_classes.get("o:id")] if resource_classes.get(
                                "o:id") in acceptable_ids else []
                        else:
                            continue

                        # Extract year from date
                        date_info = item.get('dcterms:date', [])
                        if date_info:
                            date_value = date_info[0]['@value']
                            year = date_value.split('-')[0]
                            for id in item_classes:
                                items_by_year_type[year][acceptable_ids[id]] += 1
                page += 1

    return items_by_year_type


def visualize_items_over_years(items_by_year_type):
    # Prepare data for visualization
    data = []
    for year, types in sorted(items_by_year_type.items()):  # Ensure data is sorted by year
        for type_name, count in types.items():
            data.append({'Year': year, 'Type': type_name, 'Number of Items': count})

    # Create a stacked bar chart
    fig = px.bar(data, x='Year', y='Number of Items', color='Type',
                 title='Number of Items by Type Over Years',
                 labels={'Number of Items': 'Number of Items', 'Year': 'Year', 'Type': 'Item Type'},
                 hover_data={'Number of Items': True})
    fig.update_traces(textposition='inside')
    fig.update_layout(barmode='stack', xaxis={'type': 'category', 'categoryorder': 'category ascending'})
    fig.update_traces(texttemplate='', hovertemplate='<b>%{x}</b><br>%{y} items<br>%{text}')
    fig.write_html('item_distribution_over_years.html')
    fig.show()


# Fetch data and create visualization
items_by_year_type = fetch_items()
visualize_items_over_years(items_by_year_type)
