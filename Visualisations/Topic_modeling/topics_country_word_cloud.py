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
import matplotlib
matplotlib.use('Agg')  # Use Agg backend to avoid GUI
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


def get_embeddings(texts, tokenizer, model):
    embeddings = []
    for text in tqdm(texts, desc="Generating embeddings"):
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)
        with torch.no_grad():
            outputs = model(**inputs)
        embeddings.append(outputs.last_hidden_state.mean(dim=1).squeeze().numpy())
    return np.array(embeddings)

def perform_topic_modeling(embeddings, n_topics=5, n_top_words=10):
    # Perform K-means clustering on the embeddings
    kmeans = KMeans(n_clusters=n_topics, random_state=42)
    cluster_labels = kmeans.fit_predict(embeddings)

    # Get the most representative documents for each topic
    top_docs_per_topic = []
    for topic in range(n_topics):
        topic_docs = np.where(cluster_labels == topic)[0]
        distances = np.linalg.norm(embeddings[topic_docs] - kmeans.cluster_centers_[topic], axis=1)
        top_docs = topic_docs[np.argsort(distances)[:n_top_words]]
        top_docs_per_topic.append(top_docs)

    return kmeans, top_docs_per_topic, cluster_labels


def get_top_words_for_topic(texts, top_docs):
    # Combine the text of the top documents
    topic_text = " ".join([texts[i] for i in top_docs])

    # Create a word frequency dictionary
    words = topic_text.split()
    word_freq = {}
    for word in words:
        if len(word) > 1:  # Exclude single-character words
            word_freq[word] = word_freq.get(word, 0) + 1

    # Sort by frequency and return top words
    return dict(sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:50])


def generate_word_cloud(word_freq, title, filename):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_freq)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(title)
    plt.tight_layout(pad=0)
    plt.savefig(filename, format='png')
    plt.close()


def process_country_data(country_name, item_sets, tokenizer, model):
    logging.info(f"Processing data for {country_name}")
    all_items = []
    for set_id in tqdm(item_sets, desc=f"Fetching {country_name} item sets"):
        items = get_items_by_set(set_id)
        all_items.extend(items)

    logging.info(f"Total items fetched for {country_name}: {len(all_items)}")

    texts = extract_texts(all_items)
    logging.info(f"Total texts extracted for {country_name}: {len(texts)}")

    # Get embeddings
    embeddings = get_embeddings(texts, tokenizer, model)

    # Perform topic modeling
    kmeans, top_docs_per_topic, cluster_labels = perform_topic_modeling(embeddings)

    # Generate word clouds for each topic
    for topic_idx, top_docs in enumerate(top_docs_per_topic):
        topic_word_freq = get_top_words_for_topic(texts, top_docs)
        generate_word_cloud(topic_word_freq, f"{country_name} - Topic {topic_idx + 1}",
                            f"{country_name.lower()}_topic_{topic_idx + 1}_wordcloud.png")

    return texts, cluster_labels

def main():
    # Set the number of CPU cores to use
    os.environ["LOKY_MAX_CPU_COUNT"] = str(os.cpu_count())

    benin_item_sets = [2187, 2188, 2189, 2185, 5502, 2186, 2191, 2190, 4922, 5501, 5500]
    burkina_faso_item_sets = [2200, 2215, 2214, 2207, 2201, 2199, 23273, 5503, 2209, 2210, 2213]

    # Load CamemBERT model and tokenizer
    tokenizer, model = load_camembert()

    try:
        benin_texts, benin_topics = process_country_data("Benin", benin_item_sets, tokenizer, model)
        print("Benin Topics:")
        for topic in set(benin_topics):
            print(f"Topic {topic}: {list(benin_topics).count(topic)} documents")

        burkina_faso_texts, burkina_faso_topics = process_country_data("Burkina Faso", burkina_faso_item_sets, tokenizer, model)
        print("\nBurkina Faso Topics:")
        for topic in set(burkina_faso_topics):
            print(f"Topic {topic}: {list(burkina_faso_topics).count(topic)} documents")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()