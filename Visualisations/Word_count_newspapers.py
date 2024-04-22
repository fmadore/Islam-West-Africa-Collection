import requests
import plotly.express as px
import pandas as pd
from tqdm import tqdm

# Constants
API_URL = "https://iwac.frederickmadore.com/api"
KEY_IDENTITY = "XXXXXXXXXXXXXXXXXX"
KEY_CREDENTIAL = "XXXXXXXXXXXXXXXX"
ITEM_SETS = {
    'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502],
    'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 23273],
    'Togo': [9458]
}

def format_number_with_spaces(number):
    """Format the number with a space every three digits."""
    return f"{number:,}".replace(",", " ")

def get_item_set_name(api_url, item_set_id, key_identity, key_credential):
    """Fetch item set name using its ID."""
    url = f"{api_url}/item_sets/{item_set_id}?key_identity={key_identity}&key_credential={key_credential}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['dcterms:title'][0]['@value'] if 'dcterms:title' in data else f"Newspaper {item_set_id}"
    else:
        print(f"Failed to fetch item set name for ID {item_set_id}: {response.status_code} - {response.text}")
        return f"Newspaper {item_set_id}"

def get_items_by_set(api_url, item_set_id, key_identity, key_credential):
    """Retrieve all items for a specific item set ID."""
    items = []
    page = 1
    while True:
        url = f"{api_url}/items?key_identity={key_identity}&key_credential={key_credential}&item_set_id={item_set_id}&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to retrieve items for set {item_set_id}: {response.status_code} - {response.text}")
            break
        data = response.json()
        if not data:
            break
        items.extend(data)
        page += 1
    return items

def extract_content(items, country, newspaper):
    """Extract content and compute word counts."""
    content_data = []
    for item in items:
        for value in item.get('bibo:content', []):
            if value['type'] == 'literal':
                words = value['@value'].split()
                word_count = len(words)
                content_data.append({'country': country, 'newspaper': newspaper, 'word_count': word_count})
    return content_data

# Collecting data
all_data = []
for country, sets in ITEM_SETS.items():
    for set_id in tqdm(sets, desc=f"Processing {country}"):
        newspaper = get_item_set_name(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL)
        items = get_items_by_set(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL)
        item_content = extract_content(items, country, newspaper)
        all_data.extend(item_content)

# Create DataFrame
df = pd.DataFrame(all_data)

# Aggregate data by country and newspaper
aggregated_data = df.groupby(['country', 'newspaper']).agg({'word_count': 'sum'}).reset_index()

# Create a label that includes the word count on a new line with formatted numbers
aggregated_data['label'] = aggregated_data['newspaper'] + "<br>" + aggregated_data['word_count'].apply(format_number_with_spaces) + " words"

# Calculate total word count and format it
total_word_count = format_number_with_spaces(df['word_count'].sum())

# Create treemap
fig = px.treemap(aggregated_data, path=['country', 'label'], values='word_count',
                 title=f'Total Word Count: {total_word_count} - Proportion by Country and Newspaper')

fig.show()

# Save the figure as HTML file
fig.write_html("treemap_word_count.html")
