import requests
import re
import stanza
import pandas as pd
import nltk
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
from nltk.corpus import stopwords
from tqdm import tqdm
from plotly.offline import plot
import plotly.express as px
import plotly.graph_objects as go

# Download necessary resources
nltk.download('stopwords')
nltk.download('punkt')

# Load French stop words
french_stopwords = set(stopwords.words('french')) | {'El', '000', '%'}
french_stopwords = set(word.lower() for word in french_stopwords)

# Initialize Stanza French model
nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt,pos,lemma')

# Compile regular expressions for text cleaning
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"’")
whitespace_re = re.compile(r"\s+")
oe_re = re.compile(r"œ")

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

def preprocess_texts(texts):
    processed_texts = []
    for text in tqdm(texts, desc="Preprocessing texts"):
        text = newline_re.sub(' ', text)
        text = apostrophe_re.sub("'", text)
        text = whitespace_re.sub(" ", text)
        text = oe_re.sub("oe", text)
        text = text.strip().lower()  # Convert to lower case before processing

        # Process the cleaned text with Stanza
        doc = nlp(text)
        tokens = [word.lemma.lower() for sent in doc.sentences for word in sent.words
                  if word.upos not in ['PUNCT', 'SYM', 'X'] and word.lemma.lower() not in french_stopwords]
        processed_text = ' '.join(tokens)
        processed_texts.append(processed_text)
    return processed_texts

def analyze_sentiments(texts):
    sentiments = []
    for text in tqdm(texts, desc="Analyzing sentiments"):
        blob = TextBlob(text, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
        polarity = blob.sentiment[0]
        subjectivity = blob.sentiment[1]
        sentiments.append((polarity, subjectivity))
    return sentiments

def create_sentiment_visualization(sentiments, file_name):
    df = pd.DataFrame(sentiments, columns=['Polarity', 'Subjectivity'])
    fig = px.scatter(df, x='Polarity', y='Subjectivity', title="Sentiment Analysis: Polarity vs Subjectivity")
    plot(fig, filename=file_name)

def main():
    benin_item_sets = [2188]
    burkina_faso_item_sets = [2200]

    benin_items = fetch_items_from_set(benin_item_sets)
    burkina_faso_items = fetch_items_from_set(burkina_faso_item_sets)

    benin_texts = extract_texts(benin_items)
    burkina_faso_texts = extract_texts(burkina_faso_items)

    benin_processed = preprocess_texts(benin_texts)
    burkina_faso_processed = preprocess_texts(burkina_faso_texts)

    benin_sentiments = analyze_sentiments(benin_processed)
    burkina_faso_sentiments = analyze_sentiments(burkina_faso_processed)

    create_sentiment_visualization(benin_sentiments, 'sentiment_visualization_benin.html')
    create_sentiment_visualization(burkina_faso_sentiments, 'sentiment_visualization_burkina_faso.html')

if __name__ == "__main__":
    main()
