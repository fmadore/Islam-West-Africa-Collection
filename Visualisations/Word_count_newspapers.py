import requests
from tqdm import tqdm

def count_words_in_articles(api_url):
    total_word_count = 0
    page = 1
    more_pages_available = True

    while more_pages_available:
        # Construct the API request URL
        response = requests.get(f"{api_url}/items", params={
            'resource_class_label': 'Article',  # Filter by Article type
            'page': page,
            'per_page': 50  # Adjust based on how many items you want to retrieve per page
        })
        data = response.json()

        # Check if the page has items; if not, stop the loop
        if not data:
            more_pages_available = False
            continue

        # Initialize the progress bar
        progress_bar = tqdm(data, desc=f'Processing page {page}')

        # Process each item
        for item in progress_bar:
            if "@type" in item and "bibo:Article" in item["@type"]:
                for content in item.get('bibo:content', []):
                    if content["type"] == "literal" and content["is_public"] and "@value" in content:
                        word_count = len(content["@value"].split())
                        total_word_count += word_count

        page += 1

    return total_word_count

# API URL setup
api_url = "https://iwac.frederickmadore.com/api"
total_words = count_words_in_articles(api_url)
print("Total number of words in 'bibo:content' of all 'bibo:Article' items:", total_words)
