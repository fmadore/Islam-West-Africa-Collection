import requests
import plotly.express as px
from collections import defaultdict, Counter
from tqdm import tqdm

api_url = "https://iwac.frederickmadore.com/api"
resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305, 58, 49, 36, 60, 38]  # Resource class IDs

def fetch_language_labels(language_ids):
    """ Fetch language labels for a set of language resource IDs. """
    labels_en = {}
    labels_fr = {}
    for language_id in tqdm(language_ids, desc="Fetching language labels"):
        response = requests.get(f"{api_url}/items/{language_id}")
        if response.status_code == 200:
            item_data = response.json()
            for d in item_data.get('dcterms:title', []):
                if d["@language"] == "en":
                    labels_en[language_id] = d["@value"]
                elif d["@language"] == "fr":
                    labels_fr[language_id] = d["@value"]
    return labels_en, labels_fr

def fetch_items_by_class(resource_classes):
    """ Fetch all items for specified classes, collecting language resource IDs. """
    language_count = Counter()
    page = 1
    with tqdm(desc="Fetching items", unit=" pages") as pbar:
        while True:
            response = requests.get(f"{api_url}/items", params={"resource_class_id[]": resource_classes, "page": page, "per_page": 50})
            data = response.json()
            if not data:
                break
            for item in data:
                languages = item.get('dcterms:language', [])
                for lang in languages:
                    language_count[lang['value_resource_id']] += 1
            page += 1
            pbar.update(1)
    return language_count

def create_pie_chart(language_labels, language_count, title, filename):
    """ Create a pie chart from the language distribution data and save as HTML. """
    labels = [language_labels.get(id, "Unknown") for id in language_count]
    values = list(language_count.values())

    fig = px.pie(names=labels, values=values, title=title)
    fig.write_html(filename)  # Save the figure as an HTML file
    fig.show()

# Fetch items and aggregate by language IDs
language_distribution = fetch_items_by_class(resource_classes)

# Fetch labels for the accumulated language IDs
labels_en, labels_fr = fetch_language_labels(language_distribution.keys())

# Generate visualizations in both languages
create_pie_chart(labels_en, language_distribution, 'Distribution of items by language', 'distribution_by_language_en.html')
create_pie_chart(labels_fr, language_distribution, 'Répartition des éléments par langue', 'distribution_by_language_fr.html')
