import requests
from tqdm import tqdm
from nltk.corpus import stopwords
import nltk
import spacy
from collections import Counter
import json
import re
import os
from dotenv import load_dotenv

# Get the current script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from .env file in the root directory
root_dir = os.path.dirname(os.path.dirname(script_dir))
load_dotenv(os.path.join(root_dir, '.env'))

# NLTK and spaCy setup
nltk.download('stopwords')
french_stopwords = set(stopwords.words('french'))
additional_stopwords = {'El', '000', '%'}
french_stopwords.update(additional_stopwords)
french_stopwords = set(word.lower() for word in french_stopwords)

# Load French language model
nlp = spacy.load('fr_core_news_sm')

API_URL = os.getenv('OMEKA_BASE_URL')
KEY_IDENTITY = os.getenv('OMEKA_KEY_IDENTITY')
KEY_CREDENTIAL = os.getenv('OMEKA_KEY_CREDENTIAL')
ITEM_SETS = {
    'Bénin': [2185],
    'Burkina Faso': [2199],
    'Togo': [9458]
}

# Regular expressions for text cleanup
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"'")
whitespace_re = re.compile(r'\s+')
oe_re = re.compile(r'œ')

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

        # Processing text with spaCy
        doc = nlp(text)
        tokens = [token.lemma_ for token in doc 
                  if token.lemma_ not in french_stopwords and not token.is_punct and not token.is_space]

        processed_texts.extend(tokens)
    return processed_texts

def get_word_frequencies(texts, top_n=100):
    """Get word frequencies from processed texts."""
    word_freq = Counter(texts)
    return word_freq.most_common(top_n)

# Collect and preprocess data
all_word_frequencies = {}
for country, sets in ITEM_SETS.items():
    country_texts = []
    for set_id in tqdm(sets, desc=f"Processing {country}"):
        items = get_items_by_set(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL)
        for item in items:
            for value in item.get('bibo:content', []):
                if value['type'] == 'literal':
                    country_texts.append(value['@value'])
    preprocessed_texts = preprocess_texts(country_texts)
    all_word_frequencies[country] = get_word_frequencies(preprocessed_texts)

# Generate JSON files for each country
for country, word_freq in all_word_frequencies.items():
    json_data = [{"text": word, "size": freq} for word, freq in word_freq]
    
    filename = f"{country.lower().replace(' ', '_')}_word_frequencies.json"
    file_path = os.path.join(script_dir, filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"Word frequencies for {country} saved to {file_path}")

# Generate a combined JSON file for all countries
combined_json_data = {country: [{"text": word, "size": freq} for word, freq in word_freq] 
                      for country, word_freq in all_word_frequencies.items()}

combined_file_path = os.path.join(script_dir, "combined_word_frequencies.json")
with open(combined_file_path, 'w', encoding='utf-8') as f:
    json.dump(combined_json_data, f, ensure_ascii=False, indent=2)
print(f"Combined word frequencies saved to {combined_file_path}")