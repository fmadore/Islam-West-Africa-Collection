from flask import Flask, request, jsonify, render_template
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import os
from dotenv import load_dotenv
import logging
import anthropic

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


def search_documents(query, max_results=5):
    try:
        query_vector = vectorizer.transform([query])
        cosine_similarities = cosine_similarity(query_vector, tfidf_matrix).flatten()
        top_indices = cosine_similarities.argsort()[-max_results:][::-1]
        return [documents[i] for i in top_indices]
    except Exception as e:
        logging.error(f"Error in search_documents: {str(e)}")
        return []


def prepare_context(relevant_docs, max_tokens=3000):
    try:
        context = ""
        for doc in relevant_docs:
            doc_content = f"Title: {doc['title']}\nSubject: {', '.join(doc['subject'])}\nDate: {doc['date']}\nContent: {doc['content'][:500]}...\n\n"
            if len(context) + len(doc_content) > max_tokens:
                break
            context += doc_content
        return context
    except Exception as e:
        logging.error(f"Error in prepare_context: {str(e)}")
        return ""


def query_ai(context, user_question):
    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1000,  # Increased from 10 to 1000
            temperature=0,
            system="You are an AI assistant for the Islam West Africa Collection (IWAC). Use the provided context to answer questions about Islam in West Africa. Respond in the same language as the user's question.",
            messages=[
                {
                    "role": "user",
                    "content": f"Context:\n{context}\n\nHuman: {user_question}"
                }
            ]
        )
        return message.content[0].text
    except Exception as e:
        logging.error(f"Error in query_ai: {str(e)}")
        return f"Error querying AI: {str(e)}"


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/chat', methods=['POST'])
def chat():
    try:
        user_question = request.json['question']
        logging.info(f"Received question: {user_question}")

        relevant_docs = search_documents(user_question)
        logging.info(f"Found {len(relevant_docs)} relevant documents")

        context = prepare_context(relevant_docs)
        logging.info(f"Prepared context of length: {len(context)}")

        ai_response = query_ai(context, user_question)
        logging.info(f"Received AI response of length: {len(ai_response)}")

        return jsonify({"response": ai_response})
    except Exception as e:
        logging.error(f"Error in chat endpoint: {str(e)}")
        return jsonify({"response": f"An error occurred: {str(e)}"}), 500


if __name__ == '__main__':
    app.run(debug=True)