import requests
from collections import defaultdict
from tqdm import tqdm
import plotly.graph_objects as go
import os

# API URL and item set identifiers
api_url = "https://islam.zmo.de/api"
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

def fetch_author_id(author_name):
    """Fetch the o:id for a given author name."""
    params = {
        "property[0][property]": 1,
        "property[0][type]": "eq",
        "property[0][text]": author_name
    }
    response = requests.get(f"{api_url}/items", params=params)
    data = response.json()
    if data:
        return data[0].get('o:id')
    return None

def create_top_authors_chart(author_count, country, top_n=20, language='fr'):
    """ Create a bar chart for top authors' publication counts with hyperlinks and save as HTML. """
    sorted_authors = sorted(author_count.items(), key=lambda x: x[1], reverse=True)
    top_authors = sorted_authors[:top_n]
    
    authors, counts = zip(*top_authors)
    
    # Fetch o:id for each author
    author_ids = []
    for author in tqdm(authors, desc="Fetching author IDs"):
        author_ids.append(fetch_author_id(author))
    
    # Create hyperlinks
    base_url = "https://islam.zmo.de/s/afrique_ouest/item/" if language == 'fr' else "https://islam.zmo.de/s/westafrica/item/"
    hyperlinks = [f"{base_url}{id}" if id else "" for id in author_ids]
    
    # Translations
    translations = {
        'fr': {
            'title': f"Top {top_n} des auteurs par nombre de publications pour {country}",
            'x_axis': 'Nombre de publications',
            'y_axis': 'Auteur(e)',
            'legend': 'Nombre de publications'
        },
        'en': {
            'title': f"Top {top_n} authors by publication count for {country}",
            'x_axis': 'Number of Publications',
            'y_axis': 'Author',
            'legend': 'Publication Count'
        }
    }
    
    # Select the appropriate language
    text = translations[language]
    
    fig = go.Figure(data=[
        go.Bar(
            x=counts,
            y=[f'<a href="{link}" target="_blank">{author}</a>' if link else author for author, link in zip(authors, hyperlinks)],
            orientation='h',
            marker=dict(
                color=counts,
                colorscale='Viridis',
                colorbar=dict(title=text['legend'])
            )
        )
    ])
    
    fig.update_layout(
        title=text['title'],
        xaxis_title=text['x_axis'],
        yaxis_title=text['y_axis'],
        height=800,
        yaxis=dict(autorange="reversed")
    )
    
    # Create the "Authors" folder if it doesn't exist
    authors_folder = os.path.join(os.path.dirname(__file__), "Authors")
    os.makedirs(authors_folder, exist_ok=True)

    # Save the figure as an HTML file in the "Authors" folder
    filename = f"top_authors_{country.replace(' ', '_').lower()}_{language}.html"
    filepath = os.path.join(authors_folder, filename)
    fig.write_html(filepath)
    
    print(f"Chart saved as {filepath}")

def main():
    """ Main function to process each country's data set and create a combined chart. """
    all_author_count = defaultdict(int)
    for item_set_id, country in country_item_sets.items():
        items = fetch_items(item_set_id)
        author_count = parse_authors(items)
        create_top_authors_chart(author_count, country, language='fr')
        create_top_authors_chart(author_count, country, language='en')
        for author, count in author_count.items():
            all_author_count[author] += count
    create_top_authors_chart(all_author_count, "tous les pays", language='fr')
    create_top_authors_chart(all_author_count, "all countries", language='en')

if __name__ == "__main__":
    main()
