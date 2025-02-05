"""
This script generates word cloud data from text content stored in an Omeka S database.
It processes text content from different item sets grouped by country (Bénin, Burkina Faso, and Togo),
performs text preprocessing and cleaning, and outputs word frequency data in JSON format.

The script:
1. Connects to an Omeka S API to fetch item content
2. Processes text using NLP techniques (spaCy and NLTK)
3. Removes stopwords, punctuation, and unwanted tokens
4. Generates word frequencies for each country
5. Outputs both individual country files and a combined JSON file

Requirements:
- Python 3.x
- spaCy with French language model (fr_dep_news_trf)
- NLTK with French stopwords
- Environment variables for Omeka S API credentials
"""

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

# Omeka S API configuration
API_URL = os.getenv('OMEKA_BASE_URL')
KEY_IDENTITY = os.getenv('OMEKA_KEY_IDENTITY')
KEY_CREDENTIAL = os.getenv('OMEKA_KEY_CREDENTIAL')

# Dictionary mapping countries to their respective item set IDs in Omeka S
ITEM_SETS = {
    'Bénin': [2185, 2185, 5502, 2186, 2188, 2187, 2191, 2190, 2189, 4922, 5501, 5500, 60638],
    'Burkina Faso': [2199, 2200, 23273, 5503, 2215, 2214, 2207, 2209, 2210, 2213, 2201],
    'Togo': [25304, 9458, 5498, 5499, 67399, 67407, 67460, 67430, 67456]
}

# Set up NLP tools and resources
nltk.download('stopwords')
french_stopwords = set(stopwords.words('french'))

# Extend stopwords with additional custom words and common French verbs
additional_stopwords = {'El', '000', '%', "être", "avoir", "faire", "dire", "aller", "voir", "savoir", 
                       "pouvoir", "falloir", "vouloir", "m."}
french_stopwords.update(additional_stopwords)

# Load French language model for spaCy
nlp = spacy.load('fr_dep_news_trf')

# Combine NLTK and spaCy stopwords
spacy_french_stopwords = nlp.Defaults.stop_words
french_stopwords.update(spacy_french_stopwords)
french_stopwords = set(word.lower() for word in french_stopwords)

# Add common French contractions to stopwords
contractions = {"d'", "l'", "n'", "qu'", "j'", "t'", "s'", "m'"}
french_stopwords.update(contractions)

# Compile regular expressions for text cleanup
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"['']")  # Matches both curly and straight apostrophes
whitespace_re = re.compile(r'\s+')
oe_re = re.compile(r'œ')

def get_items_by_set(api_url, item_set_id, key_identity, key_credential):
    """
    Retrieve all items from a specific Omeka S item set using pagination.
    
    Args:
        api_url (str): Base URL of the Omeka S API
        item_set_id (int): ID of the item set to retrieve
        key_identity (str): API key identity
        key_credential (str): API key credential
    
    Returns:
        list: List of items from the specified item set
    """
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
    """
    Clean and preprocess text data using NLP techniques.
    
    Processing steps:
    1. Basic text cleaning (newlines, apostrophes, whitespace)
    2. Tokenization using spaCy
    3. Lemmatization
    4. Removal of stopwords, punctuation, and unwanted tokens
    
    Args:
        texts (list): List of text strings to process
    
    Returns:
        list: Processed and cleaned tokens
    """
    processed_texts = []
    for text in tqdm(texts, desc="Preprocessing texts"):
        # Basic text normalization
        text = newline_re.sub(' ', text)
        text = apostrophe_re.sub("'", text)
        text = whitespace_re.sub(" ", text)
        text = oe_re.sub("oe", text)
        text = text.strip().lower()

        # Process text with spaCy NLP pipeline
        doc = nlp(text)
        tokens = []
        for token in doc:
            # Apply multiple filtering criteria
            if (token.lemma_.lower() not in french_stopwords 
                and not token.is_punct 
                and not token.is_space
                and len(token.lemma_) > 1
                and not token.is_stop
                and token.pos_ not in ['ADP', 'DET', 'PRON', 'AUX', 'SCONJ', 'CCONJ']
                and not token.is_digit
                and not token.like_num):
                
                # Skip contractions
                if token.text.lower() in contractions:
                    continue
                
                lemma = token.lemma_.lower()
                
                # Additional validation checks
                if (lemma not in french_stopwords 
                    and "'" not in lemma 
                    and not lemma.startswith("'") 
                    and not lemma.endswith("'")
                    and not lemma.isdigit()):
                    tokens.append(lemma)

        processed_texts.extend(tokens)
    return processed_texts

def get_word_frequencies(texts, top_n=150):
    """
    Calculate word frequencies from processed texts.
    
    Args:
        texts (list): List of processed tokens
        top_n (int): Number of top frequent words to return
    
    Returns:
        list: List of tuples (word, frequency) sorted by frequency
    """
    word_freq = Counter(texts)
    return word_freq.most_common(top_n)

# Main execution block
# Process each country's data and generate word frequencies
all_word_frequencies = {}
for country, sets in ITEM_SETS.items():
    country_texts = []
    # Collect texts from all item sets for the current country
    for set_id in tqdm(sets, desc=f"Processing {country}"):
        items = get_items_by_set(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL)
        for item in items:
            for value in item.get('bibo:content', []):
                if value['type'] == 'literal':
                    country_texts.append(value['@value'])
    
    # Process texts and get word frequencies
    preprocessed_texts = preprocess_texts(country_texts)
    all_word_frequencies[country] = get_word_frequencies(preprocessed_texts, top_n=150)

# Generate individual JSON files for each country
for country, word_freq in all_word_frequencies.items():
    json_data = [{"text": word, "size": freq} for word, freq in word_freq]
    
    filename = f"{country.lower().replace(' ', '_')}_word_frequencies.json"
    file_path = os.path.join(script_dir, filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"Word frequencies for {country} saved to {file_path}")

# Generate combined JSON file containing data for all countries
combined_json_data = {country: [{"text": word, "size": freq} for word, freq in word_freq] 
                      for country, word_freq in all_word_frequencies.items()}

combined_file_path = os.path.join(script_dir, "combined_word_frequencies.json")
with open(combined_file_path, 'w', encoding='utf-8') as f:
    json.dump(combined_json_data, f, ensure_ascii=False, indent=2)
print(f"Combined word frequencies saved to {combined_file_path}")