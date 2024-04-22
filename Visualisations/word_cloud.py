import requests
import matplotlib.pyplot as plt
from tqdm import tqdm
from nltk.corpus import stopwords
import nltk
import stanza
from wordcloud import WordCloud
import seaborn as sns
import re

# NLTK and Stanza setup
nltk.download('stopwords')
french_stopwords = set(stopwords.words('french'))
additional_stopwords = {'El', '000', '%'}
french_stopwords.update(additional_stopwords)
french_stopwords = set(word.lower() for word in french_stopwords)

nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt,pos,lemma')

API_URL = "https://iwac.frederickmadore.com/api"
KEY_IDENTITY = "XXXXXXX"
KEY_CREDENTIAL = "XXXXXXX"
ITEM_SETS = {
    'Bénin': [2185],
    'Burkina Faso': [2199],
    'Togo': [9458]
}

# Regular expressions for text cleanup
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"’")
whitespace_re = re.compile(r'\s+')
oe_re = re.compile(r'œ')

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


def preprocess_texts(texts):
    """Preprocess and clean texts for further processing."""
    processed_texts = []
    for text in tqdm(texts, desc="Preprocessing texts"):
        text = newline_re.sub(' ', text)  # Replaces newlines with spaces
        text = apostrophe_re.sub("'", text)  # Normalizes apostrophes
        text = whitespace_re.sub(" ", text)  # Collapses multiple spaces
        text = oe_re.sub("oe", text)  # Replaces special oe ligature with oe
        text = text.strip().lower()  # Convert to lower case and strip whitespace

        # Processing text with Stanza
        doc = nlp(text)
        tokens = [word.lemma if word.lemma is not None else '' for sent in doc.sentences for word in sent.words if
                  word.lemma not in french_stopwords]

        # Join tokens into a single string for the word cloud, filtering out empty strings
        processed_text = ' '.join(filter(None, tokens))

        processed_texts.append(processed_text)
    return processed_texts


# Collect and preprocess data
all_texts = {}
for country, sets in ITEM_SETS.items():
    country_texts = []
    for set_id in tqdm(sets, desc=f"Processing {country}"):
        items = get_items_by_set(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL)
        for item in items:
            for value in item.get('bibo:content', []):
                if value['type'] == 'literal':
                    country_texts.append(value['@value'])
    preprocessed_texts = preprocess_texts(country_texts)
    all_texts[country] = ' '.join(preprocessed_texts)

# Generate and plot word clouds
for country, texts in all_texts.items():
    wordcloud = WordCloud(width=800, height=400).generate(texts)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.title(f'Word Cloud for {country}')
    plt.axis('off')
    plt.show()
