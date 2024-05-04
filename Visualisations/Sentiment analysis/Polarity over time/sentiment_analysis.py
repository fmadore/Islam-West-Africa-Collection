from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
from tqdm import tqdm

def analyze_sentiments(texts):
    """Analyze sentiments of given texts.

    Args:
        texts (list): List of preprocessed texts to analyze.

    Returns:
        list: List of polarity scores.
    """
    sentiments = []
    for text in tqdm(texts, desc="Analyzing sentiments"):
        blob = TextBlob(text, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
        polarity = blob.sentiment[0]
        sentiments.append(polarity)
    return sentiments
