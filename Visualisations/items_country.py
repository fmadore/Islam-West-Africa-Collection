import requests
from collections import defaultdict
import plotly.express as px
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
session = requests.Session()

item_set_ids = [
    2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 4922, 5500, 5501, 5502, 10223, 2218, 2219, 2220,
    2196, 2197, 2198, 2199, 2200, 2201, 2202, 2203, 2204, 2205, 2206, 2207, 2209, 2210, 2211, 2212, 2213, 2214, 2215,
    2216, 2217, 23452, 23453, 23273, 5503, 2222, 2223, 2184, 2225, 23253, 2226, 2227, 2228, 9458
]


def fetch_items(item_set_id):
    page = 1
    items = []
    while True:
        response = session.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        items.extend(data)
        page += 1
    return items


def get_title_by_language(titles, language):
    for title in titles:
        if title.get('@language', '') == language:
            return title['@value']
    return titles[0]['@value'] if titles else 'Unknown Set Title'


def fetch_and_categorize_items(language):
    items_by_country_and_set = defaultdict(lambda: defaultdict(int))
    errors = []

    for item_set_id in tqdm(item_set_ids, desc="Processing item sets"):
        try:
            item_set_response = session.get(f"{api_url}/item_sets/{item_set_id}")
            if item_set_response.status_code != 200:
                errors.append((item_set_id, item_set_response.status_code))
                continue

            item_set_data = item_set_response.json()
            titles = item_set_data.get('dcterms:title', [])
            set_title = get_title_by_language(titles, language)
            country = item_set_data.get('dcterms:spatial', [{}])[0].get('display_title', 'Unknown Country')

            items = fetch_items(item_set_id)
            items_by_country_and_set[country][set_title] += len(items)

        except Exception as e:
            errors.append((item_set_id, str(e)))
            continue

    if errors:
        print("Errors occurred with the following item sets and their details:", errors)

    return items_by_country_and_set


def visualize_spatial_distribution(items_by_country_and_set, language='en'):
    total_items = sum(count for country_data in items_by_country_and_set.values() for count in country_data.values())
    title_map = {
        'en': f'Distribution of the {total_items} items by country and sub-collection',
        'fr': f'Répartition des {total_items} éléments par pays et sous-collection'
    }
    title = title_map.get(language, f'Distribution of {total_items} items by country and sub-collection')
    filename = f'item_distribution_by_country_and_set_{language}.html'

    data = [{'Country': country, 'Item Set Title': set_title, 'Number of Items': count}
            for country, sets in items_by_country_and_set.items() for set_title, count in sets.items()]

    fig = px.treemap(data, path=['Country', 'Item Set Title'], values='Number of Items', title=title)
    fig.update_traces(textinfo="label+value+percent parent")
    fig.write_html(filename)
    fig.show()


# Execute the functions
for lang in ['en', 'fr']:
    items_by_country_and_set = fetch_and_categorize_items(language=lang)
    visualize_spatial_distribution(items_by_country_and_set, language=lang)
