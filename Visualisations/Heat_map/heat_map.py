import os
import requests
from tqdm.auto import tqdm
import folium
from folium.plugins import HeatMap
import pandas as pd
import logging
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path

class OmekaHeatmapGenerator:
    def __init__(self):
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup paths
        self.script_dir = Path(__file__).parent
        self.env_path = self.script_dir.parent / '.env'
        
        # Load and validate environment variables
        self._load_environment()
        
        # Initialize session for better performance
        self.session = requests.Session()
        
        # Configure rate limiting
        self.max_workers = 5
        
        # Store all coordinates for consolidated map
        self.all_coordinates = []
        
    def _load_environment(self) -> None:
        """Load and validate environment variables."""
        load_dotenv(dotenv_path=self.env_path)
        
        self.api_url = os.getenv("OMEKA_BASE_URL")
        self.key_identity = os.getenv("OMEKA_KEY_IDENTITY")
        self.key_credential = os.getenv("OMEKA_KEY_CREDENTIAL")
        
        if not all([self.api_url, self.key_identity, self.key_credential]):
            raise EnvironmentError(
                "OMEKA_BASE_URL, OMEKA_KEY_IDENTITY, and OMEKA_KEY_CREDENTIAL "
                "must be set in the .env file."
            )

    def fetch_json(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch JSON data from URL with error handling and retries."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Request failed for URL {url}: {e}")
            return None

    def fetch_items(self, item_set_id: int) -> List[Dict[str, Any]]:
        """Fetch all items from a given item set with pagination support."""
        items = []
        page = 1
        base_url = (
            f"{self.api_url}/items?"
            f"key_identity={self.key_identity}&"
            f"key_credential={self.key_credential}&"
            f"item_set_id={item_set_id}&per_page=50"
        )

        while True:
            url = f"{base_url}&page={page}"
            data = self.fetch_json(url)
            
            if not data:
                break
                
            if isinstance(data, list):
                items.extend(data)
                break
            elif isinstance(data, dict):
                items.extend(data.get('data', []))
                if not data.get('links', {}).get('next'):
                    break
                page += 1
            else:
                self.logger.error(f"Unexpected data format: {data}")
                break
                
        return items

    def fetch_coordinates(self, spatial_url: str) -> Optional[Tuple[float, float]]:
        """Extract coordinates from spatial data URL."""
        spatial_details = self.fetch_json(spatial_url)
        if not spatial_details:
            return None

        coordinates_field = spatial_details.get('curation:coordinates', [])
        if not coordinates_field:
            return None

        coord_text = coordinates_field[0].get('@value')
        if not coord_text or ',' not in coord_text:
            return None

        try:
            lat, lon = map(float, coord_text.split(','))
            return lat, lon
        except ValueError:
            self.logger.warning(f"Invalid coordinates format: {coord_text}")
            return None

    def extract_coordinates(self, items: List[Dict[str, Any]]) -> List[Tuple[float, float]]:
        """Extract coordinates from items using parallel processing."""
        coordinates = []
        spatial_urls = []
        
        # Collect all spatial URLs
        for item in items:
            for spatial in item.get('dcterms:spatial', []):
                if spatial_url := spatial.get('@id'):
                    spatial_urls.append(spatial_url)

        # Process URLs in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(tqdm(
                executor.map(self.fetch_coordinates, spatial_urls),
                desc="Extracting coordinates",
                total=len(spatial_urls)
            ))
            
        coordinates = [coord for coord in results if coord is not None]
        return coordinates

    def generate_heatmap(self, coordinates: List[Tuple[float, float]], filename: str) -> None:
        """Generate and save heatmap visualization."""
        if not coordinates:
            self.logger.info(f"No valid coordinates found for {filename}")
            return

        df = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude'])
        map_center = df.mean().tolist()
        
        m = folium.Map(
            location=map_center,
            zoom_start=4,
            tiles='CartoDB positron'
        )

        # Add heatmap layer with custom gradient
        HeatMap(
            coordinates,
            min_opacity=0.3,
            radius=15,
            blur=10,
            gradient={
                0.4: 'blue',
                0.6: 'purple',
                0.8: 'red',
                1.0: 'white'
            }
        ).add_to(m)

        # Save the map
        output_path = self.script_dir / f"{filename}.html"
        m.save(str(output_path))
        self.logger.info(f"Heatmap saved as {output_path}")

    def process_country(self, country: str, item_set_ids: List[int]) -> int:
        """Process all item sets for a country."""
        self.logger.info(f"Processing data for {country}")
        country_coordinates = []
        total_items = 0

        for item_set_id in item_set_ids:
            items = self.fetch_items(item_set_id)
            if items:
                total_items += len(items)
                coordinates = self.extract_coordinates(items)
                country_coordinates.extend(coordinates)
                self.all_coordinates.extend(coordinates)  # Add to consolidated data

        self.generate_heatmap(
            country_coordinates, 
            f"heatmap_{country.replace(' ', '_').lower()}"
        )
        self.logger.info(f"Total items processed for {country}: {total_items}")
        return total_items

    def generate_consolidated_map(self):
        """Generate a consolidated map with all data points."""
        self.logger.info("Generating consolidated IWAC heatmap...")
        self.generate_heatmap(
            self.all_coordinates,
            "heatmap_IWAC"
        )
        self.logger.info(f"Total coordinates in consolidated map: {len(self.all_coordinates)}")

def main():
    countries = {
        'Benin': [2185, 5502, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5501, 5500],
        'Burkina Faso': [2199, 2200, 2215, 2214, 2207, 2201, 23448, 5503, 2209, 2210, 2213],
        'Togo': [25304, 9458, 5498],
        'Côte d\'Ivoire': [43051, 31882, 15845, 45390]
    }

    generator = OmekaHeatmapGenerator()
    
    # Process each country
    total_global_items = sum(
        generator.process_country(country, item_set_ids)
        for country, item_set_ids in countries.items()
    )
    
    # Generate consolidated map
    generator.generate_consolidated_map()
    
    generator.logger.info(f"Total items processed globally: {total_global_items}")

if __name__ == "__main__":
    main()
