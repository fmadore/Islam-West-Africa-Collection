import re
import stanza
from tqdm import tqdm
from config import BASE_URL

# Compile regular expressions for text cleaning
newline_re = re.compile(r'\n')
apostrophe_re = re.compile(r"’")
whitespace_re = re.compile(r"\s+")
oe_re = re.compile(r"œ")

# Initialize Stanza French model
nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt,pos,lemma')

def extract_texts_and_dates(items):
    """Extract texts and corresponding dates from items.

    Args:
        items (list): List of items to process.

    Returns:
        tuple: Lists of texts and corresponding dates.
    """
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
    """Preprocess texts by cleaning and tokenizing.

    Args:
        texts (list): List of texts to preprocess.

    Returns:
        list: List of processed texts.
    """
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
                  if word.upos not in ['PUNCT', 'SYM', 'X']]
        processed_text = ' '.join(tokens)
        processed_texts.append(processed_text)
    return processed_texts
