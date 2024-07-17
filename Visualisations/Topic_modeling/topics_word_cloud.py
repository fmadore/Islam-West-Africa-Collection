import os
import shutil
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import concurrent.futures
from tqdm import tqdm
import logging
import time
import torch
from transformers import CamembertTokenizer, CamembertModel
from sklearn.cluster import KMeans
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()

# Define API credentials
API_URL = "https://iwac.frederickmadore.com/api"
KEY_IDENTITY = os.getenv("API_KEY_IDENTITY")
KEY_CREDENTIAL = os.getenv("API_KEY_CREDENTIAL")

if not KEY_IDENTITY or not KEY_CREDENTIAL:
    logging.error("API_KEY_IDENTITY and API_KEY_CREDENTIAL must be set in the .env file.")
    exit(1)

# Configure session with retry mechanism
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

# Function to clear the Hugging Face cache
def clear_huggingface_cache():
    cache_dir = os.path.expanduser("~/.cache/huggingface")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        logging.info("Hugging Face cache cleared.")
    else:
        logging.info("Hugging Face cache not found.")

# Load CamemBERT model and tokenizer
def load_camembert():
    try:
        tokenizer = CamembertTokenizer.from_pretrained("camembert-base")
        model = CamembertModel.from_pretrained("camembert-base")
    except OSError:
        logging.warning("Error loading CamemBERT. Clearing cache and trying again.")
        clear_huggingface_cache()
        tokenizer = CamembertTokenizer.from_pretrained("camembert-base", force_download=True)
        model = CamembertModel.from_pretrained("camembert-base", force_download=True)
    return tokenizer, model

tokenizer, model = load_camembert()


def fetch_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed for URL {url}: {e}")
        return None


def fetch_items_page(item_set_id, page):
    """Fetch items for a specific page of an item set."""
    url = f"{API_URL}/items?key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}&item_set_id={item_set_id}&page={page}&per_page=100"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'items' in data:
                # New API format
                items = data['items']
                has_next_page = data.get('next') is not None
            elif isinstance(data, list):
                # Old API format, list of items
                items = data
                has_next_page = len(items) == 100  # Assume there's a next page if we got 100 items
            elif isinstance(data, dict) and 'data' in data:
                # Old API format, dictionary with 'data' key
                items = data['data']
                has_next_page = 'next' in data.get('links', {})
            else:
                logging.error(f"Unexpected data format from API: {data}")
                return [], False

            return items, has_next_page
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for set {item_set_id}, page {page}: {e}")
            if attempt == max_retries - 1:
                logging.error(f"Failed to retrieve items for set {item_set_id}, page {page} after {max_retries} attempts")
                return [], False
            time.sleep(2 ** attempt)  # Exponential backoff


def get_items_by_set(item_set_id):
    """Retrieve all items for a specific item set ID using threading to parallelize page fetching."""
    all_items = []
    page = 1
    has_next_page = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while has_next_page:
            future = executor.submit(fetch_items_page, item_set_id, page)
            items, has_next_page = future.result()
            all_items.extend(items)
            logging.info(f"Fetched {len(items)} items from set {item_set_id}, page {page}")
            page += 1

    logging.info(f"Total fetched {len(all_items)} items from set {item_set_id}")
    return all_items

def extract_texts(items):
    """Extract content from items."""
    texts = []
    for item in items:
        for content in item.get('bibo:content', []):
            if content.get('type') == 'literal':
                texts.append(content.get('@value', ''))
    return texts


def get_embeddings(texts):
    embeddings = []
    for text in tqdm(texts, desc="Generating embeddings"):
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)
        with torch.no_grad():
            outputs = model(**inputs)
        embeddings.append(outputs.last_hidden_state.mean(dim=1).squeeze().numpy())
    return np.array(embeddings)


def cluster_texts(embeddings, n_clusters=5):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    return kmeans.fit_predict(embeddings)


def get_top_words(texts, cluster_labels, cluster, top_n=50):
    cluster_texts = [text for text, label in zip(texts, cluster_labels) if label == cluster]
    all_words = ' '.join(cluster_texts).split()
    word_freq = {}
    for word in all_words:
        if len(word) > 1:  # Exclude single-character words
            word_freq[word] = word_freq.get(word, 0) + 1
    return dict(sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:top_n])


def generate_word_cloud(word_freq, title, filename):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_freq)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(title)
    plt.tight_layout(pad=0)
    plt.savefig(filename)
    plt.close()


def process_country_data(country_name, item_sets):
    logging.info(f"Processing data for {country_name}")
    all_items = []
    for set_id in tqdm(item_sets, desc=f"Fetching {country_name} item sets"):
        items = get_items_by_set(set_id)
        all_items.extend(items)

    logging.info(f"Total items fetched for {country_name}: {len(all_items)}")

    texts = extract_texts(all_items)
    logging.info(f"Total texts extracted for {country_name}: {len(texts)}")

    return texts


def main():
    benin_item_sets = [2187, 2188, 2189, 2185, 5502, 2186, 2191, 2190, 4922, 5501, 5500]
    burkina_faso_item_sets = [2200, 2215, 2214, 2207, 2201, 2199, 23273, 5503, 2209, 2210, 2213]

    process_country_data("Benin", benin_item_sets)
    process_country_data("Burkina Faso", burkina_faso_item_sets)

if __name__ == "__main__":
    main()