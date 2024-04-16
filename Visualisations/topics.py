import requests
import re
import stanza
from gensim import corpora, models
import pyLDAvis.gensim_models as gensimvis
import pyLDAvis
from tqdm import tqdm

# Initialize Stanza French model
nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt,pos,lemma')

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
        # Replace newlines with a space
        text = text.replace('\n', ' ')

        # Additional cleaning steps
        text = re.sub(r"’", "'", text)  # Convert curved apostrophes to straight ones
        text = re.sub(r"\s+", " ", text)  # Remove multiple spaces
        text = re.sub(r"œ", "oe", text)  # Normalize "oe"
        text = text.strip()  # Strip spaces at the beginning and end

        # Process the cleaned text with Stanza
        doc = nlp(text)
        tokens = [word.lemma for sent in doc.sentences for word in sent.words if not word.upos in ['PUNCT', 'SYM', 'X']]
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
    benin_item_sets = [2187, 2188]
    burkina_faso_item_sets = [2200, 2215]

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
