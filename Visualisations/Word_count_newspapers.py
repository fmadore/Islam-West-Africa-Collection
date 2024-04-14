import requests
import plotly.express as px
from tqdm import tqdm

# Define the API URL
api_url = "https://iwac.frederickmadore.com/api"

# Define country to item set IDs mapping
item_sets = {
    'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502],
    'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 23273],
    'Togo': [9458]
}

def fetch_item_set_title(item_set_id):
    """ Fetches the title for a given item set ID. """
    response = requests.get(f"{api_url}/item_sets/{item_set_id}")
    data = response.json()
    if 'dcterms:title' in data and len(data['dcterms:title']) > 0:
        return data['dcterms:title'][0]['@value']
    return "Unknown Title"

def fetch_word_counts(item_set_ids):
    """ Fetches word counts for the given item set IDs, with progress bar. """
    word_counts = {}
    for id in tqdm(item_set_ids, desc="Fetching titles and word counts"):
        title = fetch_item_set_title(id)
        page = 1
        more_pages_available = True
        total_words = 0
        while more_pages_available:
            response = requests.get(f"{api_url}/items", params={'item_set_id': id, 'page': page, 'per_page': 50})
            data = response.json()
            if not data:
                more_pages_available = False
                continue
            for item in data:
                for content in item.get('bibo:content', []):
                    if content["type"] == "literal" and content["is_public"] and "@value" in content:
                        word_count = len(content["@value"].split())
                        total_words += word_count
            page += 1
        word_counts[title] = total_words
    return word_counts

def create_interactive_treemap(data):
    """ Creates an interactive treemap using Plotly. """
    fig = px.treemap(
        data,
        path=['country', 'title'],
        values='word_count',
        title='Word Count Proportion by Country and Newspaper'
    )
    fig.data[0].textinfo = 'label+text+value'
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    fig.write_html('treemap.html')  # Save as HTML file for easy embedding
    fig.show()

# Prepare data structure for Plotly
data = []
for country, ids in item_sets.items():
    print(f"Processing {country}")
    word_counts = fetch_word_counts(ids)
    for title, count in word_counts.items():
        data.append({'country': country, 'title': title, 'word_count': count})

create_interactive_treemap(data)
