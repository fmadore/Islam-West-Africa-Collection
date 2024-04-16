import nltk
import requests
import re
import stanza
from gensim import corpora, models
import pyLDAvis.gensim_models as gensimvis
import pyLDAvis
from nltk.corpus import stopwords
from tqdm import tqdm

# Download necessary NLTK resources
nltk.download('stopwords')

# Load French stop words
french_stopwords = set(stopwords.words('french'))
additional_stopwords = {'El', '000'}  # Add any other words to remove
french_stopwords.update(additional_stopwords)
french_stopwords = set(word.lower() for word in french_stopwords)  # Ensure all stopwords are lowercase

# Initialize Stanza French model
nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt,pos,lemma')

# Compile regular expressions
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

def perform_lda(texts):
    dictionary = corpora.Dictionary([text.split() for text in texts])
    corpus = [dictionary.doc2bow(text.split()) for text in texts]
    lda_model = models.LdaModel(corpus, num_topics=5, id2word=dictionary, passes=15, update_every=1, chunksize=100, iterations=50)
    return lda_model, corpus, dictionary

def create_visualization(lda_model, corpus, dictionary, file_name):
    vis = gensimvis.prepare(lda_model, corpus, dictionary)
    pyLDAvis.save_html(vis, file_name)

def main():
    benin_item_sets = [2187, 2188, 2189]
    burkina_faso_item_sets = [2200, 2215, 2214, 2207, 2201]

    benin_items = fetch_items_from_set(benin_item_sets)
    burkina_faso_items = fetch_items_from_set(burkina_faso_item_sets)

    benin_texts = extract_texts(benin_items)
    burkina_faso_texts = extract_texts(burkina_faso_items)

    benin_processed = preprocess_texts(benin_texts)
    burkina_faso_processed = preprocess_texts(burkina_faso_texts)

    benin_lda_model, benin_corpus, benin_dictionary = perform_lda(benin_processed)
    burkina_faso_lda_model, burkina_faso_corpus, burkina_faso_dictionary = perform_lda(burkina_faso_processed)

    create_visualization(benin_lda_model, benin_corpus, benin_dictionary, 'lda_visualization_benin.html')
    create_visualization(burkina_faso_lda_model, burkina_faso_corpus, burkina_faso_dictionary, 'lda_visualization_burkina_faso.html')

if __name__ == "__main__":
    main()
