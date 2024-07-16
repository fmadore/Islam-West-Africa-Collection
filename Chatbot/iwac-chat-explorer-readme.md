# IWAC Chat Explorer

IWAC Chat Explorer is an AI-powered web application that implements an augmented retrieval strategy to explore the Islam West Africa Collection (IWAC). This project combines traditional information retrieval techniques with large language model capabilities to provide intelligent, context-aware responses to user queries about Islam in West Africa.

## Features

- Interactive chat interface for querying the IWAC dataset
- Augmented retrieval system combining document search and AI-generated responses
- AI-powered keyword extraction from user queries
- Relevant document search based on extracted keywords
- Context-aware responses using the Claude 3.5 Sonnet model from Anthropic
- Source attribution for AI-generated responses
- Responsive design using Tailwind CSS

## How It Works

The IWAC Chat Explorer uses an augmented retrieval strategy that involves several steps:

1. **Keyword Extraction**: When a user submits a query, the system uses AI to extract relevant keywords from the question.

2. **Document Retrieval**: Using these keywords, the system searches the preprocessed IWAC dataset to find the most relevant documents.

3. **Context Preparation**: The content of the retrieved documents is compiled into a context that the AI can use to formulate its response.

4. **AI-Powered Response Generation**: The system then sends this context along with the original user query to the Claude 3.5 Sonnet model, which generates a detailed, context-aware response.

5. **Source Attribution**: Finally, the system provides the AI-generated response to the user, along with citations of the source documents used to inform the response.

This approach combines the strengths of traditional information retrieval (finding relevant documents) with the natural language understanding and generation capabilities of large language models, resulting in informative and contextually relevant responses.

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/fmadore/Islam-West-Africa-Collection/iwac-chat-explorer.git
   cd iwac-chat-explorer
   ```

2. Set up a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   Create a `.env` file in the project root and add your Anthropic API key:
   ```
   ANTHROPIC_API_KEY=your_api_key_here
   ```

5. Prepare the IWAC dataset:
   Ensure you have the `preprocessed_iwac_data.json` file in your project directory. This file should contain the preprocessed documents from the IWAC dataset.

## Usage

1. Start the Flask application:
   ```
   python app.py
   ```

2. Open a web browser and navigate to `http://localhost:5000`

3. Use the chat interface to ask questions about Islam in West Africa. The system will retrieve relevant information from the IWAC dataset and provide AI-generated responses based on this context.

## Project Structure

- `app.py`: Main Flask application file containing the augmented retrieval logic
- `templates/index.html`: HTML template for the chat interface
- `static/`: Directory for static files (CSS, images, etc.)
- `preprocessed_iwac_data.json`: Preprocessed IWAC dataset (not included in the repository)

## Acknowledgements

- This project uses the Islam West Africa Collection (IWAC) dataset.
- AI capabilities are powered by the Anthropic Claude 3.5 Sonnet model.
- Frontend styling is done using Tailwind CSS.

