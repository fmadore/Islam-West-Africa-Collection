from flask import Flask, request, jsonify, render_template
try:
    from markupsafe import Markup
except ImportError as e:
    logging.error(f"Failed to import Markup from markupsafe: {str(e)}")
    raise

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import os
from dotenv import load_dotenv
import logging
import anthropic
import re
import tiktoken

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Load preprocessed data
try:
    with open('preprocessed_iwac_data.json', 'r', encoding='utf-8') as f:
        preprocessed_data = json.load(f)

    documents = preprocessed_data['documents']
    tfidf_matrix = preprocessed_data['tfidf_matrix']
    vectorizer = TfidfVectorizer(vocabulary=preprocessed_data['vectorizer_vocabulary'])
    logging.info("Preprocessed data loaded successfully")
except Exception as e:
    logging.error(f"Error loading preprocessed data: {str(e)}")
    documents, tfidf_matrix, vectorizer = [], [], None

# Anthropic client setup
client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

def extract_keywords_with_ai(query):
    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=100,
            temperature=0,
            system="You are an AI assistant that extracts important keywords and concepts from questions about Islam in West Africa. Respond with only the extracted keywords, separated by commas.",
            messages=[
                {
                    "role": "user",
                    "content": f"Extract important keywords and concepts from this question: {query}"
                }
            ]
        )
        keywords = [keyword.strip() for keyword in message.content[0].text.split(',')]
        logging.info(f"AI-extracted keywords: {keywords}")
        return keywords
    except Exception as e:
        logging.error(f"Error in extract_keywords_with_ai: {str(e)}")
        return []

def search_documents(keywords, max_results=5):
    try:
        # Calculate relevance scores
        relevance_scores = []
        for doc in documents:
            score = 0
            doc_text = f"{doc['title']} {' '.join(doc['subject'])} {doc['content']}"
            doc_text_lower = doc_text.lower()
            for keyword in keywords:
                score += doc_text_lower.count(keyword.lower())
            relevance_scores.append(score)

        # Get top documents
        top_indices = sorted(range(len(relevance_scores)), key=lambda i: relevance_scores[i], reverse=True)[:max_results]
        selected_docs = [documents[i] for i in top_indices if relevance_scores[i] > 0]

        logging.info(f"Keywords: {keywords}")
        for i, doc in enumerate(selected_docs):
            logging.info(f"Selected document {i + 1}:")
            logging.info(f"  Title: {doc['title']}")
            logging.info(f"  Date: {doc['date']}")
            logging.info(f"  Subjects: {', '.join(doc['subject'])}")
            logging.info(f"  Publisher: {doc.get('publisher', 'Unknown Publisher')}")  # Log the publisher
            logging.info(f"  Relevance score: {relevance_scores[top_indices[i]]}")

        return selected_docs
    except Exception as e:
        logging.error(f"Error in search_documents: {str(e)}")
        return []


def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


def prepare_context(relevant_docs, max_tokens=100000):
    try:
        context = ""
        docs_included = 0
        for doc in relevant_docs:
            doc_content = f"Source {docs_included + 1}:\nTitle: {doc['title']}\nDate: {doc['date']}\nURL: {doc['url']}\nSubject: {', '.join(doc['subject'])}\nContent: {doc['content']}\n\n"

            if num_tokens_from_string(context + doc_content) > max_tokens:
                break

            context += doc_content
            docs_included += 1

        logging.info(f"Prepared context with {docs_included} full documents. Total tokens: {num_tokens_from_string(context)}")
        return context
    except Exception as e:
        logging.error(f"Error in prepare_context: {str(e)}")
        return ""


def query_ai(context, user_question):
    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=8192,
            temperature=0.3,
            system="You are an AI assistant for the Islam West Africa Collection (IWAC). Use the provided context to answer questions about Islam in West Africa. Respond in the same language as the user's question. Provide a detailed answer, using line breaks between paragraphs for better readability. Do not cite sources or include a list of sources in your response.",
            messages=[
                {
                    "role": "user",
                    "content": f"Context:\n{context}\n\nHuman: {user_question}\n\nRemember, do not include any citations or list of sources in your response."
                }
            ],
            extra_headers = {"anthropic-beta": "max-tokens-3-5-sonnet-2024-07-15"}
        )
        return message.content[0].text
    except Exception as e:
        logging.error(f"Error in query_ai: {str(e)}")
        return f"Error querying AI: {str(e)}"

def process_ai_response(response):
    # Add paragraph breaks
    processed_response = re.sub(r'\n\n', '</p><p>', f'<p>{response}</p>')

    # Mark the response as safe HTML
    return Markup(processed_response)


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/chat', methods=['POST'])
def chat():
    try:
        user_question = request.json['question']
        logging.info(f"Received question: {user_question}")

        keywords = extract_keywords_with_ai(user_question)
        logging.info(f"Extracted keywords: {keywords}")

        relevant_docs = search_documents(keywords, max_results=10)
        logging.info(f"Found {len(relevant_docs)} relevant documents")

        context = prepare_context(relevant_docs)
        logging.info(f"Prepared context of length: {len(context)}")

        ai_response = query_ai(context, user_question)
        logging.info(f"Received AI response of length: {len(ai_response)}")

        processed_response = process_ai_response(ai_response)
        logging.info(f"Processed AI response")

        # Format all relevant documents as sources
        formatted_sources = [
            {
                "title": doc['title'],
                "date": doc['date'],
                "url": doc['url'],
                "publisher": doc.get('publisher', 'Unknown Publisher')
            }
            for doc in relevant_docs
        ]

        return jsonify({
            "response": processed_response,
            "sources": formatted_sources
        })
    except Exception as e:
        logging.error(f"Error in chat endpoint: {str(e)}")
        return jsonify({"response": f"An error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    logging.info("Starting the Flask application...")
    app.run(debug=True, host='0.0.0.0', port=5000)
    logging.info("Flask application has stopped.")