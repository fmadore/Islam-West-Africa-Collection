import os
import requests
import re
from tqdm import tqdm
from dotenv import load_dotenv
from transformers import CamembertTokenizer, CamembertModel
import torch
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords

# Download French stop words
nltk.download('stopwords')
french_stop_words = stopwords.words('french')

# Define the path to the .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# Load environment variables from .env file
load_dotenv(dotenv_path=env_path)

API_URL = "https://iwac.frederickmadore.com/api"
KEY_IDENTITY = os.getenv("API_KEY_IDENTITY")
KEY_CREDENTIAL = os.getenv("API_KEY_CREDENTIAL")

# Initialize CamemBERT tokenizer and model
tokenizer = CamembertTokenizer.from_pretrained("camembert-base")
model = CamembertModel.from_pretrained("camembert-base")

# Compile regular expressions
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"'")
whitespace_re = re.compile(r"\s+")
oe_re = re.compile(r"œ")


def fetch_items_page(api_url, item_set_id, key_identity, key_credential, page):
    """Fetch items for a specific page of an item set."""
    url = f"{api_url}/items?key_identity={key_identity}&key_credential={key_credential}&item_set_id={item_set_id}&page={page}"
    response = requests.get(url)
    return response.json()


def fetch_items_from_set(item_set_ids):
    items = []
    for set_id in tqdm(item_set_ids, desc="Fetching item sets"):
        page = 1
        while True:
            data = fetch_items_page(API_URL, set_id, KEY_IDENTITY, KEY_CREDENTIAL, page)
            if not data:
                break
            items.extend(data)
            page += 1
    return items


def extract_texts(items):
    texts = []
    for item in tqdm(items, desc="Extracting texts"):
        if "bibo:content" in item:
            content_blocks = item["bibo:content"]
            for content in content_blocks:
                if content.get('property_label') == 'content' and content.get('is_public', True):
                    texts.append(content.get('@value', ''))
    return texts


def preprocess_texts(texts):
    processed_texts = []
    for text in tqdm(texts, desc="Preprocessing texts"):
        text = newline_re.sub(' ', text)
        text = apostrophe_re.sub("'", text)
        text = whitespace_re.sub(" ", text)
        text = oe_re.sub("oe", text)
        text = text.strip().lower()  # Convert to lower case before processing
        processed_texts.append(text)
    return processed_texts


def perform_topic_modeling(texts, n_topics=5):
    # Initialize BERTopic with custom French stop words and probability calculation
    vectorizer = CountVectorizer(stop_words=french_stop_words)
    topic_model = BERTopic(
        language="multilingual",
        n_gram_range=(1, 2),
        min_topic_size=5,
        nr_topics=n_topics,
        vectorizer_model=vectorizer,
        calculate_probabilities=True
    )

    # Fit the model and transform
    topics, probs = topic_model.fit_transform(texts)

    return topic_model, topics, probs


def create_visualizations(topic_model, topics, probs, country_name):
    # 1. Topic visualization
    fig = topic_model.visualize_topics()
    fig.write_html(f'topic_visualization_{country_name}.html')

    # 2. Topic distribution
    if probs is not None and len(probs) > 0:
        fig = topic_model.visualize_distribution(probs[0], min_probability=0.015)
        fig.write_html(f'topic_distribution_{country_name}.html')
    else:
        print(f"Warning: Unable to create topic distribution visualization for {country_name}")

    # 3. Topic heatmap
    fig = topic_model.visualize_heatmap(n_clusters=20)
    fig.write_html(f'topic_heatmap_{country_name}.html')

    # 4. Topic similarity
    fig = topic_model.visualize_topics_over_time(topics, timestamps=range(len(topics)))
    fig.write_html(f'topic_similarity_{country_name}.html')

    # 5. Word clouds for top topics
    topic_info = topic_model.get_topic_info()
    for i in range(min(5, len(topic_info))):
        topic_id = topic_info.iloc[i]['Topic']
        if topic_id != -1:  # Exclude the outlier topic if it's in top 5
            words = dict(topic_model.get_topic(topic_id))
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(words)
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title(f'Topic {topic_id} Word Cloud')
            plt.savefig(f'wordcloud_topic_{topic_id}_{country_name}.png')
            plt.close()

    # 6. Interactive topic chart
    topic_sizes = topic_info['Count'].values
    topic_names = [f"Topic {i}" for i in topic_info['Topic']]
    fig = go.Figure(data=[go.Bar(x=topic_names, y=topic_sizes)])
    fig.update_layout(title=f'Topic Sizes in {country_name}', xaxis_title='Topics', yaxis_title='Number of Documents')
    fig.write_html(f'topic_sizes_{country_name}.html')

    # Save topics to CSV
    topic_info.to_csv(f'topics_{country_name}.csv', index=False)


def main():
    benin_item_sets = [2187]
    burkina_faso_item_sets = [2200]

    benin_items = fetch_items_from_set(benin_item_sets)
    burkina_faso_items = fetch_items_from_set(burkina_faso_item_sets)

    benin_texts = extract_texts(benin_items)
    burkina_faso_texts = extract_texts(burkina_faso_items)

    benin_processed = preprocess_texts(benin_texts)
    burkina_faso_processed = preprocess_texts(burkina_faso_texts)

    benin_topic_model, benin_topics, benin_probs = perform_topic_modeling(benin_processed)
    burkina_faso_topic_model, burkina_faso_topics, burkina_faso_probs = perform_topic_modeling(burkina_faso_processed)

    create_visualizations(benin_topic_model, benin_topics, benin_probs, "Benin")
    create_visualizations(burkina_faso_topic_model, burkina_faso_topics, burkina_faso_probs, "Burkina_Faso")


if __name__ == "__main__":
    main()