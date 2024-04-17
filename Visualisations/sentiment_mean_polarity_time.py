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

def extract_texts_and_dates(items):
    texts = []
    dates = []
    for item in tqdm(items, desc="Extracting texts and dates"):
        date_content = next((content.get('@value', '') for content in item.get('dcterms:date', []) if content.get('is_public', True)), None)
        if date_content:  # Ensure there is a date before adding the text
            if "bibo:content" in item:
                content_blocks = item["bibo:content"]
                for content in content_blocks:
                    if content.get('property_label') == 'content' and content.get('is_public', True):
                        text_content = content.get('@value', '')
                        if text_content:  # Ensure there is text content
                            texts.append(text_content)
                            dates.append(date_content)  # Only add date if there's a corresponding text
    return texts, dates

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
        sentiments.append(polarity)
    return sentiments

def create_polarity_time_series(sentiments, dates, file_name):
    df = pd.DataFrame({'Date': dates, 'Polarity': sentiments})

    # Convert date strings to datetime objects, handling different formats
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', infer_datetime_format=True)

    # Remove any rows where dates could not be converted (if any)
    df = df.dropna(subset=['Date'])

    # Group by Date and calculate mean Polarity
    df = df.groupby('Date').mean().reset_index()

    # Create the figure with a range slider
    fig = px.line(df, x='Date', y='Polarity', title="Mean polarity over time")
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(count=5, label="5Y", step="year", stepmode="backward"),
                    dict(count=10, label="10Y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )

    plot(fig, filename=file_name)

def main():
    benin_item_sets = [2187, 2188, 2189]
    burkina_faso_item_sets = [2200, 2215, 2214, 2207, 2201]

    benin_items = fetch_items_from_set(benin_item_sets)
    burkina_faso_items = fetch_items_from_set(burkina_faso_item_sets)

    benin_texts, benin_dates = extract_texts_and_dates(benin_items)
    burkina_faso_texts, burkina_faso_dates = extract_texts_and_dates(burkina_faso_items)

    benin_processed = preprocess_texts(benin_texts)
    burkina_faso_processed = preprocess_texts(burkina_faso_texts)

    benin_sentiments = analyze_sentiments(benin_processed)
    burkina_faso_sentiments = analyze_sentiments(burkina_faso_processed)

    create_polarity_time_series(benin_sentiments, benin_dates, 'polarity_time_series_benin.html')
    create_polarity_time_series(burkina_faso_sentiments, burkina_faso_dates, 'polarity_time_series_burkina_faso.html')

if __name__ == "__main__":
    main()
