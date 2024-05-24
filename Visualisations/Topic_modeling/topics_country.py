import requests
from tqdm import tqdm
from bertopic import BERTopic
from transformers import CamembertModel, CamembertTokenizer
from sklearn.feature_extraction.text import CountVectorizer
import torch
import numpy as np
import os
import nltk
from nltk.corpus import stopwords

# Download and get French stopwords
nltk.download('stopwords')
french_stopwords = stopwords.words('french')

# Fetch and Extract Texts
def fetch_items_from_set(item_set_ids):
    base_url = "https://iwac.frederickmadore.com/api/items"
    items = []
    for set_id in tqdm(item_set_ids, desc="Fetching item sets"):
        page = 1
        while True:
            response = requests.get(f"{base_url}?item_set_id={set_id}&page={page}")
            data = response.json()
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

# Fetch items and extract texts
item_set_ids = [2188]
items = fetch_items_from_set(item_set_ids)
texts = extract_texts(items)

# Create embeddings using the locally cloned CamemBERT model
local_camembert_path = './camembert-base'
tokenizer = CamembertTokenizer.from_pretrained(local_camembert_path)
model = CamembertModel.from_pretrained(local_camembert_path)

def embed(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).squeeze().numpy()

embeddings = np.array([embed(text) for text in tqdm(texts, desc="Creating embeddings")])

# Topic Modeling with BERTopic
# Create a CountVectorizer with French stopwords
vectorizer_model = CountVectorizer(stop_words=french_stopwords)

# Create BERTopic model
topic_model = BERTopic(vectorizer_model=vectorizer_model)

# Fit the model on your texts with the embeddings
topics, probs = topic_model.fit_transform(texts, embeddings)

# Print topics
for topic_id, topic in topic_model.get_topics().items():
    print(f"Topic {topic_id}: {topic}")

# (Optional) Visualize Topics
import matplotlib.pyplot as plt

# Visualize the topics
fig = topic_model.visualize_topics()
fig.show()

# Visualize the topic hierarchy
hierarchical_fig = topic_model.visualize_hierarchy()
hierarchical_fig.show()

# Visualize the topic heatmap
heatmap_fig = topic_model.visualize_heatmap()
heatmap_fig.show()
