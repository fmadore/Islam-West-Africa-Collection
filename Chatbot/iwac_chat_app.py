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
            system="You are an AI assistant that extracts important keywords and concepts from questions about Islam in West Africa. Respond with only the extracted keywords in French, separated by commas. If the input is not in French, translate the keywords to French.",
            messages=[
                {
                    "role": "user",
                    "content": f"Extract important keywords and concepts from this question, and provide them in French: {query}"
                }
            ]
        )
        keywords = [keyword.strip() for keyword in message.content[0].text.split(',')]
        logging.info(f"AI-extracted French keywords: {keywords}")
        return keywords
    except Exception as e:
        logging.error(f"Error in extract_keywords_with_ai: {str(e)}")
        return []

def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def search_documents(keywords, max_tokens=90000, max_sources=15):
    try:
        # Calculate relevance scores
        relevance_scores = []
        for doc in documents:
            score = 0
            doc_text = f"{doc['title']} {' '.join(doc['subject'])} {doc['content']}"
            doc_text_lower = doc_text.lower()
            for keyword in keywords:
                score += doc_text_lower.count(keyword.lower())
            relevance_scores.append((score, doc))

        # Sort documents by relevance score in descending order
        sorted_docs = sorted(relevance_scores, key=lambda x: x[0], reverse=True)

        selected_docs = []
        current_tokens = 0

        for score, doc in sorted_docs:
            if score == 0 or len(selected_docs) >= max_sources:
                break  # Stop if we reach documents with no relevance or max sources limit

            doc_content = f"Title: {doc['title']}\nDate: {doc['date']}\nURL: {doc['url']}\nSubject: {', '.join(doc['subject'])}\nContent: {doc['content']}\n\n"
            doc_tokens = num_tokens_from_string(doc_content)

            if current_tokens + doc_tokens > max_tokens:
                break  # Stop if adding this document would exceed the token limit

            selected_docs.append(doc)
            current_tokens += doc_tokens

            logging.info(f"Added document: {doc['title']}")
            logging.info(f"Relevance score: {score}")
            logging.info(f"Document tokens: {doc_tokens}")
            logging.info(f"Total tokens so far: {current_tokens}")

        logging.info(f"Selected {len(selected_docs)} documents. Total tokens: {current_tokens}")
        return selected_docs

    except Exception as e:
        logging.error(f"Error in search_documents: {str(e)}")
        return []


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


def query_ai(context, user_question, model="claude-3-5-sonnet-20240620", max_tokens=8192, temperature=0.7):
    try:
        system_prompt = """You are IWAC Chat Explorer, an AI assistant for the Islam West Africa Collection (IWAC). Your purpose is to provide comprehensive, rigorous, engaging, and analytical responses to questions about Islam in West Africa based on the provided context.

Key features:
1. Respond in the same language as the user's question.
2. Provide extremely detailed, well-structured answers using line breaks between paragraphs for better readability.
3. Offer extensive temporal cues in your responses to situate events and developments in their historical context.
4. Utilize the maximum available tokens to formulate the most comprehensive responses possible. Aim to use at least 80% of the available tokens.
5. Do not explicitly cite or mention the sources used. The system will separately provide source information to the user.
6. Provide in-depth analysis, including historical background, current trends, and potential future implications when relevant.
7. Include specific examples, case studies, and comparative analyses between different regions or time periods when applicable.
8. Discuss various perspectives or interpretations on the topic, if they exist in the provided context.
9. Conclude with thought-provoking questions or areas for further exploration related to the topic.

Remember to maintain academic rigor while presenting information in an engaging and accessible manner. Your responses should not only inform but also encourage further inquiry and critical thinking about the subject matter. Base your answers on the provided context without explicitly referencing or citing the sources. If you're unsure about any information, indicate this clearly in your response."""

        message = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[
                {
                    "role": "user",
                    "content": f"Context:\n{context}\n\nHuman: {user_question}\n\nPlease provide a comprehensive and detailed response, using as much of the available token limit as possible to thoroughly explore the topic."
                }
            ],
            extra_headers={"anthropic-beta": "max-tokens-3-5-sonnet-2024-07-15"}
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

        relevant_docs = search_documents(keywords)  # No need to specify max_results
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