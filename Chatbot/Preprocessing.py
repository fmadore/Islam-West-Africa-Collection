import pandas as pd
import json
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from tqdm import tqdm
from nltk.corpus import stopwords
import nltk

# Download French stopwords
nltk.download('stopwords', quiet=True)
french_stopwords = set(stopwords.words('french'))

# Load the CSV file
print("Loading CSV file...")
df = pd.read_csv('IWAC_data.csv')

# Prepare the documents
print("Preparing documents...")
documents = []
for _, row in tqdm(df.iterrows(), total=len(df)):
    doc = {
        'title': row['dcterms:title'] if pd.notna(row['dcterms:title']) else '',
        'subject': row['dcterms:subject'].split('|') if pd.notna(row['dcterms:subject']) else [],
        'publisher': row['dcterms:publisher'] if pd.notna(row['dcterms:publisher']) else '',
        'date': row['dcterms:date'] if pd.notna(row['dcterms:date']) else '',
        'spatial': row['dcterms:spatial'].split('|') if pd.notna(row['dcterms:spatial']) else [],
        'content': row['bibo:content'] if pd.notna(row['bibo:content']) else ''
    }
    documents.append(doc)

# Create a combined text field for TF-IDF
print("Creating combined text for TF-IDF...")
combined_texts = [
    f"{doc['title']} {' '.join(doc['subject'])} {' '.join(doc['spatial'])} {doc['content']}"
    for doc in tqdm(documents)
]

# TF-IDF Vectorization
print("Performing TF-IDF vectorization...")
vectorizer = TfidfVectorizer(stop_words=list(french_stopwords))
tfidf_matrix = vectorizer.fit_transform(combined_texts)

# Convert TF-IDF matrix to list for JSON serialization
print("Converting TF-IDF matrix to list...")
tfidf_matrix_list = tfidf_matrix.toarray().tolist()

# Prepare the output data
print("Preparing output data...")
output_data = {
    'documents': documents,
    'tfidf_matrix': tfidf_matrix_list,
    'vectorizer_vocabulary': vectorizer.vocabulary_
}

# Save preprocessed data to JSON file
print("Saving preprocessed data to JSON file...")
with open('preprocessed_iwac_data.json', 'w', encoding='utf-8') as f:
    json.dump(output_data, f, ensure_ascii=False, indent=2)

print("Preprocessing complete. Data saved to 'preprocessed_iwac_data.json'")