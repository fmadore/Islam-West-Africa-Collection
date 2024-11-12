import requests
from collections import defaultdict
from tqdm import tqdm
import plotly.graph_objects as go
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

# API URL and item set identifiers
@dataclass
class Config:
    api_url: str = "https://islam.zmo.de/api"
    country_item_sets: Dict[int, str] = field(default_factory=lambda: {
        2193: 'Benin',
        2212: 'Burkina Faso',
        2217: 'Côte d\'Ivoire',
        2222: 'Niger',
        2225: 'Nigeria',
        2228: 'Togo'
    })
    translations: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        'fr': {
            'title': "Top {} des auteurs par nombre de publications pour {}",
            'x_axis': 'Nombre de publications',
            'y_axis': 'Auteur(e)',
            'legend': 'Nombre de publications'
        },
        'en': {
            'title': "Top {} authors by publication count for {}",
            'x_axis': 'Number of Publications',
            'y_axis': 'Author',
            'legend': 'Publication Count'
        }
    })
    base_urls: Dict[str, str] = field(default_factory=lambda: {
        'fr': "https://islam.zmo.de/s/afrique_ouest/item/",
        'en': "https://islam.zmo.de/s/westafrica/item/"
    })

class APIClient:
    def __init__(self, api_url: str):
        self.api_url = api_url
        
    def fetch_items(self, item_set_id: int) -> List[dict]:
        items = []
        page = 1
        total_pages = 1
        
        pbar = tqdm(total=total_pages, desc=f"Fetching items for ID {item_set_id}")
        
        while page <= total_pages:
            response = requests.get(
                f"{self.api_url}/items",
                params={"item_set_id": item_set_id, "page": page, "per_page": 50}
            )
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
    
    def fetch_author_id(self, author_name: str) -> Optional[int]:
        params = {
            "property[0][property]": 1,
            "property[0][type]": "eq",
            "property[0][text]": author_name
        }
        response = requests.get(f"{self.api_url}/items", params=params)
        data = response.json()
        return data[0].get('o:id') if data else None

class ChartCreator:
    def __init__(self, config: Config, api_client: APIClient):
        self.config = config
        self.api_client = api_client
        
    def create_top_authors_chart(self, author_count: Dict[str, int], country: str, 
                               top_n: int = 20, language: str = 'fr') -> None:
        sorted_authors = sorted(author_count.items(), key=lambda x: x[1], reverse=True)
        top_authors = sorted_authors[:top_n]
        authors, counts = zip(*top_authors)
        
        # Fetch o:id for each author
        author_ids = []
        for author in tqdm(authors, desc="Fetching author IDs"):
            author_ids.append(self.api_client.fetch_author_id(author))
        
        # Create hyperlinks
        base_url = self.config.base_urls[language]
        hyperlinks = [f"{base_url}{id}" if id else "" for id in author_ids]
        
        text = self.config.translations[language]
        self.title = text['title'].format(top_n, country)
        
        fig = self._create_figure(authors, counts, hyperlinks, text)
        self._save_figure(fig, country, language)
        
    def _create_figure(self, authors, counts, hyperlinks, text):
        return go.Figure(data=[
            go.Bar(
                x=counts,
                y=[f'<a href="{link}" target="_blank">{author}</a>' if link else author 
                   for author, link in zip(authors, hyperlinks)],
                orientation='h',
                marker=dict(
                    color=counts,
                    colorscale='Viridis',
                    colorbar=dict(
                        title=text['legend'],
                        titleside="right",
                        thickness=20
                    )
                ),
                hovertemplate=(
                    "<b>%{y}</b><br>" +
                    text['legend'] + ": %{x:,.0f}<br>" +
                    "<extra></extra>"
                )
            )
        ]).update_layout(
            title=dict(
                text=self.title,
                x=0.5,  # Center the title
                y=0.95,
                font=dict(size=24)
            ),
            xaxis_title=dict(
                text=text['x_axis'],
                font=dict(size=16)
            ),
            yaxis_title=dict(
                text=text['y_axis'],
                font=dict(size=16)
            ),
            height=800,
            width=1200,  # Fixed width for better readability
            yaxis=dict(
                autorange="reversed",
                tickfont=dict(size=14)  # Larger font for author names
            ),
            xaxis=dict(
                tickfont=dict(size=14),  # Larger font for numbers
                gridcolor='lightgrey',
                showgrid=True
            ),
            plot_bgcolor='white',  # White background
            paper_bgcolor='white',
            margin=dict(l=10, r=10, t=80, b=10),  # Adjust margins
            showlegend=False,  # Hide legend since we have a colorbar
            hoverlabel=dict(
                bgcolor="white",
                font_size=14,
                font_family="Arial"
            )
        )
    
    def _save_figure(self, fig, country: str, language: str) -> None:
        authors_folder = os.path.join(os.path.dirname(__file__), "Authors")
        os.makedirs(authors_folder, exist_ok=True)
        
        filename = f"top_authors_{country.replace(' ', '_').lower()}_{language}.html"
        filepath = os.path.join(authors_folder, filename)
        fig.write_html(filepath)
        logging.info(f"Chart saved as {filepath}")

def parse_authors(items: List[dict]) -> Dict[str, int]:
    """ Parse the authors from items and count their occurrences. """
    author_count = defaultdict(int)
    for item in items:
        authors = item.get('bibo:authorList', [])
        for author in authors:
            display_title = author.get('display_title', 'Unknown')
            author_count[display_title] += 1
    return dict(author_count)

def main():
    config = Config()
    api_client = APIClient(config.api_url)
    chart_creator = ChartCreator(config, api_client)
    
    all_author_count = defaultdict(int)
    
    for item_set_id, country in config.country_item_sets.items():
        items = api_client.fetch_items(item_set_id)
        author_count = parse_authors(items)
        
        for language in ['fr', 'en']:
            chart_creator.create_top_authors_chart(author_count, country, language=language)
            
        for author, count in author_count.items():
            all_author_count[author] += count
    
    chart_creator.create_top_authors_chart(all_author_count, "tous les pays", language='fr')
    chart_creator.create_top_authors_chart(all_author_count, "all countries", language='en')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
