import json
from pathlib import Path
from collections import defaultdict
import logging
from typing import Dict, List
import os
import sys
from tqdm import tqdm

# Add the parent directory to sys.path to allow imports
sys.path.append(str(Path(__file__).parent.parent))

# Import the existing classes from items_country.py
from items_country import (
    APIConfig, APIClient, DataProcessor, ItemSetResult, 
    clear_cache, prompt_cache_usage
)

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class D3DataVisualizer:
    def __init__(self):
        self.output_dir = Path(__file__).parent
        
        # Create cache directory in the same folder as the script
        self.cache_dir = self.output_dir / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        logger.info(f"Initialized D3DataVisualizer with output directory: {self.output_dir}")
        logger.info(f"Cache directory set to: {self.cache_dir}")
        
        # Define country names in both languages
        self.country_names = {
            'en': {
                'Bénin': 'Benin',
                'Burkina Faso': 'Burkina Faso',
                'Côte d\'Ivoire': 'Côte d\'Ivoire',
                'Niger': 'Niger',
                'Togo': 'Togo',
                'Nigeria': 'Nigeria',
                'Nigéria': 'Nigeria'
            },
            'fr': {
                'Bénin': 'Bénin',
                'Burkina Faso': 'Burkina Faso',
                'Côte d\'Ivoire': 'Côte d\'Ivoire',
                'Niger': 'Niger',
                'Togo': 'Togo',
                'Nigeria': 'Nigéria',
                'Nigéria': 'Nigéria'
            }
        }
        
        self.country_colors = {
            'Burkina Faso': '#1E4D8C',
            'Côte d\'Ivoire': '#B22222',
            'Bénin': '#006400',
            'Benin': '#006400',
            'Togo': '#4B0082',
            'Niger': '#8B4513',
            'Nigeria': '#008080',
            'Nigéria': '#008080',
        }

    def prepare_d3_data(self, items_by_country_and_set: Dict, language: str = 'en') -> dict:
        """Convert the data into a hierarchical structure for D3.js"""
        logger.info(f"Preparing D3 data for language: {language}")
        
        total_items = sum(
            count for country_data in items_by_country_and_set.values() 
            for count in country_data.values()
        )
        logger.info(f"Total items count: {total_items}")

        d3_data = {
            "name": "root",
            "children": []
        }

        for country, sets in items_by_country_and_set.items():
            translated_country = self.country_names[language][country]
            country_total = sum(sets.values())
            logger.debug(f"Processing country: {translated_country} with {country_total} items")
            
            country_data = {
                "name": translated_country,
                "color": self.country_colors[country],
                "children": []
            }

            # Sort sets by count in descending order
            sorted_sets = dict(sorted(sets.items(), key=lambda x: x[1], reverse=True))
            
            for set_title, count in sorted_sets.items():
                country_data["children"].append({
                    "name": set_title,
                    "value": count,
                    "color": self.country_colors[country]
                })

            d3_data["children"].append(country_data)

        return {
            "data": d3_data,
            "total_items": total_items
        }

    def create_visualization_data(self, items_by_country_and_set: Dict, language: str = 'en'):
        """Create and save the JSON data file for D3.js visualization"""
        logger.info(f"Creating visualization data for language: {language}")
        
        # Prepare the data
        viz_data = self.prepare_d3_data(items_by_country_and_set, language)
        
        # Save the data as JSON
        data_file = self.output_dir / f'treemap_data_{language}.json'
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(viz_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Data file created successfully at: {data_file}")

def main(languages: List[str]):
    logger.info("Starting data collection and visualization process")
    
    # Create config with cache directory in the script's folder
    config = APIConfig(
        cache_dir=Path(__file__).parent / "cache"
    )
    
    # Use the same item sets as in the original script
    country_item_sets = {
        'Bénin': [2185, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5500, 5501, 5502, 2195, 10223, 61062, 60638, 61063, 23452, 2192, 2193, 2194],
        'Burkina Faso': [2199, 2200, 2201, 2207, 2209, 2210, 2213, 2214, 2215, 5503, 23273, 2197, 2196, 2206, 2198, 2203, 2205, 2204, 2202, 23453, 23448, 23449, 2211, 2212],
        'Côte d\'Ivoire': [23253, 43622, 39797, 45829, 45390, 31882, 57953, 57952, 57951, 57950, 57949, 57948, 57945, 57944, 57943, 62076, 61684, 61320, 61289, 43051, 48249, 15845, 2216, 2217],
        'Niger': [2223, 2218, 2219, 62021, 2220, 2222],
        'Togo': [9458, 2226, 5499, 5498, 26319, 25304, 26327, 2227, 2228],
        'Nigeria': [2184, 2225]
    }

    # Calculate total number of items to process
    total_items = sum(len(items) for items in country_item_sets.values())
    logger.info(f"Processing {total_items} item sets across {len(country_item_sets)} countries")

    use_cache = prompt_cache_usage()
    if not use_cache:
        logger.info("Clearing cache as per user request")
        clear_cache(config.cache_dir)
    
    client = APIClient(config)
    client.use_cache = use_cache
    processor = DataProcessor()
    visualizer = D3DataVisualizer()

    # Process data for all languages
    items_by_country_and_set = {lang: defaultdict(lambda: defaultdict(int)) for lang in languages}
    errors = []

    # Process each country with progress bar
    for country, item_set_ids in country_item_sets.items():
        logger.info(f"Processing country: {country}")
        for item_set_id in tqdm(item_set_ids, desc=f"Processing {country}", unit="set"):
            result, error = processor.process_item_set(client, item_set_id)
            if error:
                errors.append((item_set_id, error))
                logger.warning(f"Error processing item set {item_set_id}: {error}")
            elif result:
                for lang in languages:
                    items_by_country_and_set[lang][country][result.set_titles[lang]] += result.item_count

    # Create visualization data for each language
    for language in languages:
        logger.info(f"Creating D3.js visualization data for language: {language}")
        visualizer.create_visualization_data(items_by_country_and_set[language], language)

    if errors:
        logger.warning(f"Encountered {len(errors)} errors during processing:")
        for item_set_id, error in errors:
            logger.warning(f"Item set {item_set_id}: {error}")
    
    logger.info("Data processing and visualization creation completed successfully")

if __name__ == "__main__":
    main(['en', 'fr']) 