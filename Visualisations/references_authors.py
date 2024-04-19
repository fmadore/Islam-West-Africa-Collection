import requests
from collections import defaultdict
from tqdm import tqdm
import plotly.express as px

# API URL and item set identifiers
api_url = "https://iwac.frederickmadore.com/api"
country_item_sets = {
    2193: 'Benin',
    2212: 'Burkina Faso',
    2217: 'Côte d\'Ivoire',
    2222: 'Niger',
    2225: 'Nigeria',
    2228: 'Togo'
}

def fetch_items(item_set_id):
    """ Fetch items from the API based on the item set ID. """
    items = []
    page = 1
    total_pages = 1  # Initially assumed to be 1
    pbar = tqdm(total=total_pages, desc=f"Fetching items for ID {item_set_id}")
    while page <= total_pages:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        if page == 1:
            total_result_header = response.headers.get('Omeka-S-Total-Results', str(len(data)))
            if total_result_header.isdigit():
                total_pages = int(total_result_header) // 50 + 1
            pbar.total = total_pages
        items.extend(data)
        page += 1
        pbar.update()
    pbar.close()
    return items

def parse_authors(items):
    """ Parse the authors from items and count their occurrences. """
    author_count = defaultdict(int)
    for item in items:
        authors = item.get('bibo:authorList', [])
        for author in authors:
            display_title = author.get('display_title', 'Unknown')
            author_count[display_title] += 1
    return dict(author_count)

def create_bubble_chart(author_count, country):
    """ Create and display a bubble chart for author publication counts. """
    data = [{'Author': author, 'Publications': count} for author, count in author_count.items()]
    fig = px.scatter(data, x="Author", y="Publications",
                     size="Publications", color="Author",
                     hover_name="Author", size_max=60,
                     title=f"Publication Count by Author for {country}")
    fig.update_layout(xaxis_title='Author',
                      yaxis_title='Number of Publications',
                      xaxis={'categoryorder':'total descending'})
    fig.show()

def main():
    """ Main function to process each country's data set. """
    for item_set_id, country in country_item_sets.items():
        items = fetch_items(item_set_id)
        author_count = parse_authors(items)
        create_bubble_chart(author_count, country)

if __name__ == "__main__":
    main()
