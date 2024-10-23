import os
import requests
from tqdm.auto import tqdm
import folium
import geopandas as gpd
import logging
from dotenv import load_dotenv
from collections import Counter
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import pandas as pd
from pathlib import Path

class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass

class APIError(Exception):
    """Custom exception for API-related errors."""
    pass

class SpatialDataProcessor:
    def __init__(self, env_path: Optional[str] = None):
        """Initialize the spatial data processor with configuration."""
        self._setup_logging()
        self._load_environment(env_path)
        self._validate_configuration()
        
    def _setup_logging(self) -> None:
        """Configure logging with a more detailed format."""
        log_file = Path(__file__).parent / 'spatial_processing.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _load_environment(self, env_path: Optional[str]) -> None:
        """Load environment variables from the specified path or default location."""
        if not env_path:
            env_path = Path(__file__).parent.parent / '.env'
        load_dotenv(dotenv_path=env_path)
        
        self.api_url = os.getenv("OMEKA_BASE_URL")
        self.key_identity = os.getenv("OMEKA_KEY_IDENTITY")
        self.key_credential = os.getenv("OMEKA_KEY_CREDENTIAL")

    def _validate_configuration(self) -> None:
        """Validate that all required configuration variables are present."""
        if not all([self.api_url, self.key_identity, self.key_credential]):
            raise ConfigurationError(
                "Missing required environment variables. Please ensure OMEKA_BASE_URL, "
                "OMEKA_KEY_IDENTITY, and OMEKA_KEY_CREDENTIAL are set in the .env file."
            )

    @lru_cache(maxsize=100)
    def fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch JSON data from URL with caching and error handling."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Request failed for URL {url}: {str(e)}")
            raise APIError(f"Failed to fetch data: {str(e)}")

    def fetch_items(self, item_set_id: int, per_page: int = 100) -> List[Dict]:
        """Fetch items from the API with pagination."""
        items = []
        page = 1
        
        while True:
            url = (f"{self.api_url}/items"
                   f"?key_identity={self.key_identity}"
                   f"&key_credential={self.key_credential}"
                   f"&item_set_id={item_set_id}"
                   f"&per_page={per_page}&page={page}")
            
            try:
                data = self.fetch_json(url)
                if not data:
                    break

                if isinstance(data, list):
                    items.extend(data)
                    if len(data) < per_page:
                        break
                elif isinstance(data, dict):
                    items.extend(data.get('data', []))
                    if 'next' not in data.get('links', {}):
                        break
                
                self.logger.info(f"Fetched page {page} for item set {item_set_id}")
                page += 1
                
            except APIError as e:
                self.logger.error(f"Error fetching page {page} for item set {item_set_id}: {str(e)}")
                break

        self.logger.info(f"Total items fetched for item set {item_set_id}: {len(items)}")
        return items

    def fetch_coordinates(self, spatial_url: str) -> Optional[Tuple[float, float]]:
        """Fetch and parse coordinates from spatial URL."""
        try:
            spatial_details = self.fetch_json(spatial_url)
            if not spatial_details:
                return None

            coordinates_field = spatial_details.get('curation:coordinates', [])
            if not coordinates_field:
                return None

            coord_text = coordinates_field[0].get('@value', '')
            if not coord_text or ',' not in coord_text:
                return None

            lat, lon = map(float, coord_text.split(','))
            return lat, lon

        except (ValueError, APIError) as e:
            self.logger.warning(f"Error processing coordinates from {spatial_url}: {str(e)}")
            return None

    def extract_coordinates(self, items: List[Dict]) -> List[Tuple[float, float]]:
        """Extract coordinates from items using parallel processing."""
        spatial_coverage_count = Counter()
        items_with_spatial_data = 0

        # First pass: count occurrences
        for item in tqdm(items, desc="Counting spatial coverage"):
            spatial_data = item.get('dcterms:spatial', [])
            if spatial_data:
                items_with_spatial_data += 1
                for spatial in spatial_data:
                    spatial_url = spatial.get('@id')
                    if spatial_url:
                        spatial_coverage_count[spatial_url] += 1

        # Second pass: fetch coordinates in parallel
        coordinates = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {
                executor.submit(self.fetch_coordinates, url): (url, count)
                for url, count in spatial_coverage_count.items()
            }
            
            for future in tqdm(future_to_url, desc="Fetching coordinates"):
                url, count = future_to_url[future]
                try:
                    coords = future.result()
                    if coords:
                        coordinates.extend([coords] * count)
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {str(e)}")

        return coordinates

    def load_geojson(self, country: str) -> Optional[gpd.GeoDataFrame]:
        """Load and process GeoJSON data with improved error handling."""
        data_dir = Path("data")
        if not data_dir.exists():
            self.logger.error(f"Data directory not found: {data_dir}")
            return None

        filename_patterns = [
            f"{country.lower().replace(' ', '_')}_prefectures",
            f"{country.lower().replace(' ', '_')}_administrative_boundaries_level6_counties_polygon",
            f"{country.lower().replace(' ', '_')}_administrative_boundaries_broad_districts_polygon",
            f"{country.lower().replace(' ', '_')}_Prefectures_level_2"
        ]

        for pattern in filename_patterns:
            for file in data_dir.glob(f"{pattern}*.geojson"):
                try:
                    gdf = gpd.read_file(file)
                    gdf = self._process_geojson_columns(gdf)
                    if 'name' in gdf.columns:
                        return gdf
                except Exception as e:
                    self.logger.error(f"Error loading {file}: {str(e)}")

        return None

    def _process_geojson_columns(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Process and standardize GeoJSON columns."""
        if 'properties' in gdf.columns and isinstance(gdf['properties'].iloc[0], dict):
            gdf['name'] = gdf['properties'].apply(lambda x: x.get('name') or x.get('shape2', 'Unknown'))
        elif 'shape2' in gdf.columns:
            gdf['name'] = gdf['shape2']
        return gdf

    def generate_choropleth(self, gdf: gpd.GeoDataFrame, country: str) -> None:
        """Generate an enhanced choropleth map with additional features."""
        bounds = gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2

        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=8,
            tiles='CartoDB positron'
        )

        # Add choropleth layer with quantile scale
        choropleth = folium.Choropleth(
            geo_data=gdf.__geo_interface__,
            name="choropleth",
            data=gdf,
            columns=['name', 'count'],
            key_on='feature.properties.name',
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Item Count",
            bins=8,
            reset=True
        ).add_to(m)

        # Add hover functionality with enhanced styling
        style_function = lambda x: {
            'fillColor': '#ffffff',
            'color': '#000000',
            'fillOpacity': 0.1,
            'weight': 0.1
        }
        
        highlight_function = lambda x: {
            'fillColor': '#000000',
            'color': '#000000',
            'fillOpacity': 0.50,
            'weight': 0.1
        }

        info = folium.features.GeoJson(
            gdf,
            style_function=style_function,
            control=False,
            highlight_function=highlight_function,
            tooltip=folium.features.GeoJsonTooltip(
                fields=['name', 'count'],
                aliases=['Prefecture', 'Item Count'],
                style=("background-color: white; color: #333333; "
                      "font-family: arial; font-size: 12px; padding: 10px;")
            )
        )
        
        m.add_child(info)
        m.keep_in_front(info)

        # Save the map
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        html_file_path = output_dir / f"choropleth_{country.replace(' ', '_').lower()}_prefectures.html"
        m.save(str(html_file_path))
        self.logger.info(f"Choropleth map saved as {html_file_path}")

    def process_country_data(self, country: str, item_set_ids: List[int]) -> None:
        """Process data for a single country."""
        try:
            self.logger.info(f"Processing data for {country}")
            
            # Fetch items
            all_items = []
            for item_set_id in item_set_ids:
                items = self.fetch_items(item_set_id)
                all_items.extend(items)

            # Extract coordinates
            coordinates = self.extract_coordinates(all_items)
            self.logger.info(f"Extracted {len(coordinates)} coordinates from {len(all_items)} items")

            # Load and process GeoJSON
            gdf = self.load_geojson(country)
            if gdf is None:
                raise ValueError(f"Failed to load GeoJSON for {country}")

            # Create points GeoDataFrame
            points = gpd.GeoDataFrame(
                geometry=gpd.points_from_xy([c[1] for c in coordinates], [c[0] for c in coordinates]),
                crs="EPSG:4326"
            )

            # Perform spatial join and count
            gdf = gdf.to_crs("EPSG:4326")
            joined = gpd.sjoin(points, gdf, how="inner", predicate="within")
            item_counts = joined.groupby('name').size().reset_index(name='count')

            # Merge counts with original GeoDataFrame
            gdf = gdf.merge(item_counts, on='name', how="left")
            gdf['count'] = gdf['count'].fillna(0)

            # Generate visualization
            self.generate_choropleth(gdf, country)
            
        except Exception as e:
            self.logger.error(f"Error processing {country}: {str(e)}")

def main():
    """Main function to process spatial data for multiple countries."""
    countries = {
        'Benin': [2185, 5502, 2186, 2188, 2187, 2191, 2190, 2189, 4922, 5501, 5500],
        'Burkina Faso': [2199, 2200, 23448, 23273, 23449, 5503, 2215, 2214, 2207, 2209, 2210, 2213, 2201],
        'Togo': [9458, 25304, 5498, 5499]
    }

    try:
        processor = SpatialDataProcessor()
        for country, item_set_ids in countries.items():
            processor.process_country_data(country, item_set_ids)
            
    except ConfigurationError as e:
        logging.error(f"Configuration error: {str(e)}")
        exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
