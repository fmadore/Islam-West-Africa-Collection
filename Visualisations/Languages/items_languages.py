import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import plotly.express as px
from collections import Counter
from tqdm import tqdm
import os
import logging
from typing import Dict, Tuple, List, Set
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APIClient:
    """Handle API connections with retry logic and timeout."""
    def __init__(self, base_url: str, timeout: int = 10, max_retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    
    def get(self, endpoint: str, params: dict = None) -> dict:
        """Make GET request to API with error handling."""
        try:
            response = self.session.get(
                f"{self.base_url}/{endpoint}",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise

class LanguageAnalyzer:
    """Analyze language distribution in items from an API."""
    
    def __init__(self, api_url: str, resource_classes: List[int]):
        self.api_client = APIClient(api_url)
        self.resource_classes = resource_classes
        self.output_dir = os.path.dirname(os.path.abspath(__file__))
    
    def fetch_language_labels(self, language_ids: Set[int]) -> Tuple[Dict[int, str], Dict[int, str]]:
        """
        Fetch language labels for a set of language resource IDs.
        
        Args:
            language_ids: Set of language resource IDs to fetch labels for
            
        Returns:
            Tuple of dictionaries containing English and French labels
        """
        labels_en = {}
        labels_fr = {}
        
        for language_id in tqdm(language_ids, desc="Fetching language labels"):
            try:
                item_data = self.api_client.get(f"items/{language_id}")
                
                for title in item_data.get('dcterms:title', []):
                    if title["@language"] == "en":
                        labels_en[language_id] = title["@value"]
                    elif title["@language"] == "fr":
                        labels_fr[language_id] = title["@value"]
                        
            except Exception as e:
                logger.error(f"Failed to fetch labels for ID {language_id}: {str(e)}")
                labels_en[language_id] = f"Unknown ({language_id})"
                labels_fr[language_id] = f"Inconnu ({language_id})"
                
        return labels_en, labels_fr

    def fetch_items_by_class(self) -> Counter:
        """
        Fetch all items for specified classes and count language occurrences.
        
        Returns:
            Counter object with language ID counts
        """
        language_count = Counter()
        page = 1
        
        with tqdm(desc="Fetching items", unit=" pages") as pbar:
            while True:
                try:
                    data = self.api_client.get("items", params={
                        "resource_class_id[]": self.resource_classes,
                        "page": page,
                        "per_page": 50
                    })
                    
                    if not data:
                        break
                        
                    for item in data:
                        for lang in item.get('dcterms:language', []):
                            language_count[lang['value_resource_id']] += 1
                            
                    page += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to fetch page {page}: {str(e)}")
                    break
                    
        return language_count

    def create_pie_chart(self, language_labels: Dict[int, str], 
                        language_count: Counter, title: str, 
                        filename: str) -> None:
        """
        Create and save a pie chart of language distribution.
        
        Args:
            language_labels: Dictionary mapping language IDs to labels
            language_count: Counter object with language counts
            title: Chart title
            filename: Output filename
        """
        labels = [language_labels.get(id, f"Unknown ({id})") for id in language_count]
        values = list(language_count.values())
        
        fig = px.pie(
            names=labels, 
            values=values, 
            title=title  # Removed the timestamp from the title
        )
        
        # Add timestamp to hover text
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>" +
            "Count: %{value}<br>" +
            "Percentage: %{percent}<br>" +
            "<extra></extra>"
        )
        
        # Save the figure
        output_path = os.path.join(self.output_dir, filename)
        try:
            fig.write_html(output_path)
            logger.info(f"Chart saved to {output_path}")
            fig.show()
        except Exception as e:
            logger.error(f"Failed to save chart: {str(e)}")

def main():
    """Main execution function."""
    api_url = "https://islam.zmo.de/api"
    resource_classes = [35, 43, 88, 40, 82, 178, 52, 77, 305, 58, 49, 36, 60, 38]
    
    try:
        # Initialize analyzer
        analyzer = LanguageAnalyzer(api_url, resource_classes)
        
        # Fetch and analyze data
        language_distribution = analyzer.fetch_items_by_class()
        if not language_distribution:
            logger.error("No language distribution data found")
            return
            
        # Fetch labels
        labels_en, labels_fr = analyzer.fetch_language_labels(language_distribution.keys())
        
        # Generate visualizations
        analyzer.create_pie_chart(
            labels_en, 
            language_distribution,
            'Distribution of items by language',
            'distribution_by_language_en.html'
        )
        
        analyzer.create_pie_chart(
            labels_fr, 
            language_distribution,
            'Répartition des éléments par langue',
            'distribution_by_language_fr.html'
        )
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()
