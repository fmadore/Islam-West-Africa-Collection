import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"

# Define class IDs for acceptable item types
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
    items_by_year = defaultdict(int)

    with tqdm(desc="Fetching items", unit=" page") as pbar:
        while more_pages_available:
            response = requests.get(f"{api_url}/items", params={'page': page, 'per_page': 50})
            data = response.json()
            if not data:
                more_pages_available = False
            else:
                # Update the progress bar with each completed page
                pbar.update(1)
                for item in data:
                    resource_classes = item.get('o:resource_class')
                    if resource_classes:
                        if isinstance(resource_classes, list):
                            item_classes = [rclass.get("o:id") for rclass in resource_classes]
                        elif isinstance(resource_classes, dict):
                            item_classes = [resource_classes.get("o:id")]
                        else:
                            continue  # Skip if the structure is neither list nor dict

                        # Check if the item class ID matches any of the acceptable IDs
                        if any(id in acceptable_ids for id in item_classes):
                            date_info = item.get('dcterms:date', [])
                            if date_info:
                                date_value = date_info[0]['@value']
                                if '/' in date_value:  # Handle intervals
                                    year = date_value.split('/')[0].split('-')[0]
                                else:  # Handle normal dates
                                    year = date_value.split('-')[0]
                                items_by_year[year] += 1
                page += 1

    return items_by_year

def visualize_items_over_years(items_by_year, filename):
    years = sorted(items_by_year.keys())  # Sort years to ensure the chart is ordered
    counts = [items_by_year[year] for year in years]

    data = {
        'Year': years,
        'Number of Items': counts
    }

    fig = px.bar(data, x='Year', y='Number of Items', title='Number of Items Over Years by Resource Class')
    fig.update_layout(xaxis_title='Year', yaxis_title='Number of Items', xaxis={'type': 'category'})
    fig.write_html(filename)
    fig.show()

# Fetch data and create visualization
items_by_year = fetch_items()
visualize_items_over_years(items_by_year, 'items_over_years.html')
