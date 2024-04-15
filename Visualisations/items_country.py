import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"

item_set_ids = [
    2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 4922, 5500, 5501, 5502, 10223,
    2196, 2197, 2198, 2199, 2200, 2201, 2202, 2203, 2204, 2205, 2206, 2207, 2209, 2210, 2211, 2212, 2213, 2214, 2215, 5503, 23273,
    2216, 2217,
    2218, 2219, 2220, 2222, 2223,
    2184, 2225,
    2226, 2227, 2228, 9458
]

def fetch_items(item_set_id):
    page = 1
    items = []
    while True:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        items.extend(data)
        page += 1
    return items

def fetch_and_categorize_items():
    items_by_country_and_set = defaultdict(lambda: defaultdict(int))
    errors = []

    for item_set_id in tqdm(item_set_ids, desc="Processing item sets"):
        try:
            # Fetch the item set details
            item_set_response = requests.get(f"{api_url}/item_sets/{item_set_id}")
            if item_set_response.status_code != 200:
                errors.append((item_set_id, item_set_response.status_code))
                continue

            item_set_data = item_set_response.json()
            set_title = item_set_data.get('o:title', 'Unknown Set Title')
            country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown Country')

            # Fetch all items for this item set considering pagination
            items = fetch_items(item_set_id)
            items_by_country_and_set[country][set_title] += len(items)

        except Exception as e:
            errors.append((item_set_id, str(e)))
            continue

    if errors:
        print("Errors occurred with the following item sets and their details:", errors)

    return items_by_country_and_set

def visualize_spatial_distribution(items_by_country_and_set, language='en'):
    title = 'Distribution of Items by Country and Item Set' if language == 'en' else 'Répartition des articles par pays et collections'
    filename = 'item_distribution_by_country_and_set_english.html' if language == 'en' else 'item_distribution_by_country_and_set_french.html'

    data = []
    for country, sets in items_by_country_and_set.items():
        for set_title, count in sets.items():
            data.append({'Country': country, 'Item Set Title': set_title, 'Number of Items': count})

    fig = px.treemap(data, path=['Country', 'Item Set Title'], values='Number of Items',
                     title=title)
    fig.update_traces(textinfo="label+value+percent parent")
    fig.write_html(filename)
    fig.show()

# Execute the functions
items_by_country_and_set = fetch_and_categorize_items()
visualize_spatial_distribution(items_by_country_and_set, language='en')
visualize_spatial_distribution(items_by_country_and_set, language='fr')
