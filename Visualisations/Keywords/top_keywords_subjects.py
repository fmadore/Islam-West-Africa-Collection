import os
import requests
import pandas as pd
import plotly.graph_objs as go
import logging
from tqdm.auto import tqdm
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from datetime import datetime
import backoff
from pathlib import Path
from dataclasses import dataclass
from requests.exceptions import RequestException

# Configuration
@dataclass
class Config:
    api_url: str = "https://islam.zmo.de/api"
    max_workers: int = 5
    max_retries: int = 3
    timeout: int = 30
    output_dir: str = os.path.dirname(os.path.abspath(__file__))
    logger: logging.Logger = None
    
    def __post_init__(self):
        if self.logger is None:
            self.logger = setup_logging()

# Set up logging with more detailed configuration
def setup_logging(log_file: str = "data_processing.log") -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # File handler
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_file)
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

# Custom exception for API errors
class APIError(Exception):
    pass

# Retry decorator for API calls
@backoff.on_exception(
    backoff.expo,
    (RequestException, APIError),
    max_tries=Config.max_retries
)
def fetch_data(api_url: str, item_set_id: str, timeout: int = Config.timeout, logger: logging.Logger = None) -> List[Dict[Any, Any]]:
    """
    Fetch data from a single item set with retry mechanism and error handling.
    """
    items = []
    page = 1
    
    try:
        while True:
            response = requests.get(
                f"{api_url}/items",
                params={"item_set_id": item_set_id, "page": page},
                timeout=timeout
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
                
            items.extend(data)
            page += 1
            
        return items
        
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"JSON decode error for item set {item_set_id}: {e}")
        raise APIError(f"Invalid JSON response for item set {item_set_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for item set {item_set_id}: {e}")
        raise

def process_items(items: List[Dict[Any, Any]], logger: logging.Logger = None) -> pd.DataFrame:
    """
    Process items into a DataFrame with error handling and data validation.
    """
    processed_data = []
    
    for item in items:
        try:
            subjects = [
                sub['display_title'] 
                for sub in item.get('dcterms:subject', []) 
                if isinstance(sub, dict) and sub.get('display_title')
            ]
            
            date_info = item.get('dcterms:date', [{}])[0].get('@value')
            if not date_info:
                continue
                
            date = pd.to_datetime(date_info, errors='coerce')
            if pd.isna(date):
                continue
                
            processed_data.extend([
                {'Subject': subject, 'Date': date}
                for subject in subjects
            ])
            
        except Exception as e:
            logger.warning(f"Error processing item: {e}")
            continue
            
    return pd.DataFrame(processed_data)

def fetch_and_process_country_data(
    config: Config,
    country: str,
    item_sets: List[str]
) -> pd.DataFrame:
    """
    Fetch and process data for a country with parallel processing.
    """
    config.logger.info(f"Processing data for {country}")
    all_items = []
    
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_to_id = {
            executor.submit(fetch_data, config.api_url, id, config.timeout, config.logger): id 
            for id in item_sets
        }
        
        with tqdm(total=len(item_sets), desc=f"Fetching {country}", unit="set") as pbar:
            for future in as_completed(future_to_id):
                try:
                    items = future.result()
                    all_items.extend(items)
                    pbar.update(1)
                except Exception as e:
                    item_set_id = future_to_id[future]
                    config.logger.error(f"Failed to fetch item set {item_set_id}: {e}")
                    
    df = process_items(all_items, config.logger)
    config.logger.info(f"Processed {len(df)} records for {country}")
    return df

def create_interactive_graphs(
    df: pd.DataFrame,
    country: str,
    config: Config
) -> None:
    """
    Create interactive graphs with improved styling and error handling.
    """
    if df.empty:
        config.logger.warning(f"No data available for {country}")
        return
        
    top_keywords = Counter(df['Subject']).most_common(10)
    top_keywords = [keyword for keyword, count in top_keywords]
    
    df_top_keywords = df[df['Subject'].isin(top_keywords)]
    df_grouped = df_top_keywords.groupby(
        [df_top_keywords['Date'].dt.year, 'Subject']
    ).size().reset_index(name='Frequency')
    
    translations = {
        'en': {
            'title': f"Annual Frequency of Top 10 Keywords ({country})",
            'x_label': "Year",
            'y_label': "Frequency"
        },
        'fr': {
            'title': f"Fréquence annuelle des 10 mots-clés les plus fréquents ({country})",
            'x_label': "Année",
            'y_label': "Fréquence"
        }
    }
    
    for lang, labels in translations.items():
        fig = go.Figure()
        
        for keyword in top_keywords:
            df_keyword = df_grouped[df_grouped['Subject'] == keyword]
            fig.add_trace(go.Scatter(
                x=df_keyword['Date'],
                y=df_keyword['Frequency'],
                mode='lines+markers',
                name=keyword,
                hovertemplate=f"{keyword}<br>Year: %{{x}}<br>Frequency: %{{y}}<extra></extra>"
            ))
            
        fig.update_layout(
            title=dict(
                text=labels['title'],
                x=0.5,
                font=dict(size=20)
            ),
            xaxis=dict(
                title=labels['x_label'],
                rangeslider=dict(visible=True),
                type="date",
                gridcolor='lightgray'
            ),
            yaxis=dict(
                title=labels['y_label'],
                gridcolor='lightgray'
            ),
            paper_bgcolor='white',
            plot_bgcolor='white',
            hovermode='closest',
            legend=dict(
                bgcolor='rgba(255,255,255,0.8)',
                bordercolor='gray',
                borderwidth=1
            )
        )
        
        # Modify the filename format
        output_filename = f"top_keywords_graph_{country}_{lang}.html"
        output_path = os.path.join(config.output_dir, output_filename)
        fig.write_html(output_path, full_html=True, include_plotlyjs='cdn')
        config.logger.info(f"Graph saved: {output_path}")

def main():
    """
    Main execution function with improved error handling and logging.
    """
    config = Config()
    
    country_item_sets = {
        "Bénin": ["2187", "2188", "2189"],
        "Burkina Faso": ["2200", "2215", "2214", "2207", "2201"],
        "Togo": ["5498", "5499"],
        "Côte d'Ivoire": ["43051", "31882", "15845", "45390"]
    }
    
    for country, item_sets in country_item_sets.items():
        try:
            df = fetch_and_process_country_data(config, country, item_sets)
            create_interactive_graphs(df, country, config)
        except Exception as e:
            config.logger.error(f"Failed to process {country}: {e}")
            continue

if __name__ == "__main__":
    main()
