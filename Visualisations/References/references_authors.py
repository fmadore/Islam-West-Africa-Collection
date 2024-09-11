import requests
from collections import defaultdict
from tqdm import tqdm
import plotly.express as px
import plotly.graph_objects as go
import os

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

def create_top_authors_chart(author_count, country, top_n=20):
    """ Create a bar chart for top authors' publication counts and save as HTML. """
    sorted_authors = sorted(author_count.items(), key=lambda x: x[1], reverse=True)
    top_authors = sorted_authors[:top_n]
    
    authors, counts = zip(*top_authors)
    
    fig = go.Figure(data=[
        go.Bar(
            x=counts,
            y=authors,
            orientation='h',
            marker=dict(
                color=counts,
                colorscale='Viridis',
                colorbar=dict(title='Publication Count')
            )
        )
    ])
    
    fig.update_layout(
        title=f"Top {top_n} Authors by Publication Count for {country}",
        xaxis_title='Number of Publications',
        yaxis_title='Author',
        height=800,
        yaxis=dict(autorange="reversed")
    )
    
    # Save the figure as an HTML file
    filename = f"top_authors_{country.replace(' ', '_').lower()}.html"
    filepath = os.path.join(os.path.dirname(__file__), filename)
    fig.write_html(filepath)
    
    print(f"Chart saved as {filepath}")
    
    # Optionally, still show the figure in the browser
    # fig.show()

def main():
    """ Main function to process each country's data set and create a combined chart. """
    all_author_count = defaultdict(int)
    for item_set_id, country in country_item_sets.items():
        items = fetch_items(item_set_id)
        author_count = parse_authors(items)
        create_top_authors_chart(author_count, country)
        for author, count in author_count.items():
            all_author_count[author] += count
    create_top_authors_chart(all_author_count, "All Countries")

if __name__ == "__main__":
    main()
