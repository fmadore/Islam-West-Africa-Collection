import os
import shutil
from dotenv import load_dotenv
import requests
from tqdm import tqdm
import logging
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


def fetch_items(item_set_id):
    items = []
    page = 1
    url = f"{API_URL}/items?key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}&item_set_id={item_set_id}&per_page=50&page={page}"

    while url:
        data = fetch_json(url)
        if data is None:
            break
        if isinstance(data, list):
            items.extend(data)
            break  # If the response is a list, assume no pagination
        elif isinstance(data, dict):
            items.extend(data.get('data', []))
            next_link = data.get('links', {}).get('next', {}).get('href')
            if next_link:
                url = f"{API_URL}{next_link}&key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}"
            else:
                url = None
        else:
            logging.error(f"Unexpected data format: {data}")
            break
    return items


def fetch_items_from_set(item_set_ids):
    items = []
    for set_id in tqdm(item_set_ids, desc="Fetching item sets"):
        items.extend(fetch_items(set_id))
    return items


def extract_texts(items):
    texts = []
    for item in tqdm(items, desc="Extracting texts"):
        if "bibo:content" in item:
            content_blocks = item["bibo:content"]
            for content in content_blocks:
                if content.get('property_label') == 'content':
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
    items = fetch_items_from_set(item_sets)
    texts = extract_texts(items)
    print(f"Number of texts extracted for {country_name}: {len(texts)}")

    embeddings = get_embeddings(texts)
    cluster_labels = cluster_texts(embeddings)

    for cluster in range(5):
        top_words = get_top_words(texts, cluster_labels, cluster)
        generate_word_cloud(top_words, f"{country_name} - Topic {cluster + 1}",
                            f"{country_name.lower()}_topic_{cluster + 1}_wordcloud.png")


def main():
    benin_item_sets = [2187, 2188, 2189, 2185, 5502, 2186, 2191, 2190, 4922, 5501, 5500]
    burkina_faso_item_sets = [2200, 2215, 2214, 2207, 2201, 2199, 23273, 5503, 2209, 2210, 2213]

    process_country_data("Benin", benin_item_sets)
    process_country_data("Burkina Faso", burkina_faso_item_sets)

if __name__ == "__main__":
    main()