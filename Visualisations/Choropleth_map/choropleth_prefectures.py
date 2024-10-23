import os
import requests
from tqdm.auto import tqdm
import folium
import geopandas as gpd
import logging
from dotenv import load_dotenv
from collections import Counter
from datetime import datetime
from pathlib import Path
import time

class GeoDataProcessor:
    def __init__(self):
        # Get the script's directory
        self.script_dir = Path(__file__).parent.resolve()
        
        # Create necessary directories
        self.output_dir = self.script_dir / 'output'
        self.data_dir = self.script_dir / 'data'
        self.logs_dir = self.script_dir / 'logs'
        
        for directory in [self.output_dir, self.data_dir, self.logs_dir]:
            directory.mkdir(exist_ok=True)
            
        # Configure logging
        log_file = self.logs_dir / f'processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # Load environment variables
        self.load_env_vars()
        
        # Request configuration
        self.request_timeout = 30  # seconds
        self.max_retries = 3
        self.retry_delay = 5  # seconds

    def load_env_vars(self):
        env_path = self.script_dir / '.env'
        load_dotenv(dotenv_path=env_path)
        
        self.api_url = os.getenv("OMEKA_BASE_URL")
        self.key_identity = os.getenv("OMEKA_KEY_IDENTITY")
        self.key_credential = os.getenv("OMEKA_KEY_CREDENTIAL")
        
        if not all([self.api_url, self.key_identity, self.key_credential]):
            raise ValueError("Missing required environment variables. Check .env file.")

    def fetch_json(self, url):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.request_timeout)
                response.raise_for_status()
                return response.json()
            except requests.Timeout:
                logging.warning(f"Timeout on attempt {attempt + 1} for URL {url}")
            except requests.RequestException as e:
                logging.warning(f"Request failed on attempt {attempt + 1} for URL {url}: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        logging.error(f"Failed to fetch data after {self.max_retries} attempts: {url}")
        return None

    def fetch_items(self, item_set_id):
        items = []
        page = 1
        per_page = 100

        while True:
            url = (f"{self.api_url}/items"
                  f"?key_identity={self.key_identity}"
                  f"&key_credential={self.key_credential}"
                  f"&item_set_id={item_set_id}"
                  f"&per_page={per_page}&page={page}")
            
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
            else:
                logging.error(f"Unexpected data format: {data}")
                break

            page += 1
            logging.info(f"Fetched page {page - 1} for item set {item_set_id}")

        logging.info(f"Total items fetched for item set {item_set_id}: {len(items)}")
        return items

    def fetch_coordinates(self, spatial_url):
        spatial_details = self.fetch_json(spatial_url)
        if not spatial_details:
            return None

        coordinates_field = spatial_details.get('curation:coordinates', [])
        if not coordinates_field:
            logging.warning(f"No coordinates found in {spatial_url}")
            return None

        coord_text = coordinates_field[0].get('@value')
        if not coord_text or ',' not in coord_text:
            logging.warning(f"Invalid coordinate format: {coord_text}")
            return None

        try:
            lat, lon = map(float, coord_text.split(','))
            return lat, lon
        except ValueError:
            logging.warning(f"Invalid coordinate values: {coord_text}")
            return None

    def extract_coordinates(self, items):
        spatial_coverage_count = Counter()
        items_with_spatial_data = 0

        for item in tqdm(items, desc="Counting spatial coverage"):
            spatial_data = item.get('dcterms:spatial', [])
            if spatial_data:
                items_with_spatial_data += 1
                for spatial in spatial_data:
                    spatial_url = spatial.get('@id')
                    if spatial_url:
                        spatial_coverage_count[spatial_url] += 1

        logging.info(f"Found {len(spatial_coverage_count)} unique spatial coverage values")
        logging.info(f"{items_with_spatial_data} out of {len(items)} items have spatial data")

        coordinates = []
        unique_coords = {}
        for spatial_url, count in tqdm(spatial_coverage_count.items(), desc="Fetching coordinates"):
            coords = self.fetch_coordinates(spatial_url)
            if coords:
                unique_coords[spatial_url] = coords
                coordinates.extend([coords] * count)

        return coordinates

    def load_geojson(self, country):
        possible_filenames = [
            f"{country.lower().replace(' ', '_')}_prefectures.geojson",
            f"{country.lower().replace(' ', '_')}_administrative_boundaries_level6_counties_polygon.geojson",
            f"{country.lower().replace(' ', '_')}_administrative_boundaries_broad_districts_polygon.geojson",
            f"{country.lower().replace(' ', '_')}_Prefectures_level_2.geojson"
        ]

        for filename in possible_filenames:
            file_path = self.data_dir / filename
            if file_path.exists():
                gdf = gpd.read_file(file_path)
                
                if 'name' in gdf.columns:
                    return gdf
                if 'properties' in gdf.columns:
                    gdf['name'] = gdf['properties'].apply(lambda x: x.get('name') or x.get('shape2', 'Unknown'))
                    return gdf
                if 'shape2' in gdf.columns:
                    gdf['name'] = gdf['shape2']
                    return gdf

        logging.error(f"No suitable GeoJSON file found for {country}")
        return None

    def count_items_per_prefecture(self, coordinates, gdf):
        if not coordinates:
            logging.warning("No coordinates provided for counting items per prefecture")
            return None

        points = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy([c[1] for c in coordinates], [c[0] for c in coordinates]),
            crs="EPSG:4326"
        )
        
        gdf = gdf.to_crs("EPSG:4326")
        joined = gpd.sjoin(points, gdf, how="inner", predicate="within")
        return joined.groupby('name').size().reset_index(name='count')

    def generate_choropleth(self, gdf, country):
        bounds = gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2

        m = folium.Map(location=[center_lat, center_lon], zoom_start=8)

        choropleth = folium.Choropleth(
            geo_data=gdf.__geo_interface__,
            name="choropleth",
            data=gdf,
            columns=['name', 'count'],
            key_on='feature.properties.name',
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Item Count"
        ).add_to(m)

        folium.LayerControl().add_to(m)

        # Add tooltips
        style_function = lambda x: {'fillColor': '#ffffff', 'color': '#000000', 'fillOpacity': 0.1, 'weight': 0.1}
        highlight_function = lambda x: {'fillColor': '#000000', 'color': '#000000', 'fillOpacity': 0.50, 'weight': 0.1}
        
        info = folium.features.GeoJson(
            gdf,
            style_function=style_function,
            control=False,
            highlight_function=highlight_function,
            tooltip=folium.features.GeoJsonTooltip(
                fields=['name', 'count'],
                aliases=['Prefecture', 'Item Count'],
                style="background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            )
        )
        
        m.add_child(info)
        m.keep_in_front(info)

        output_file = self.output_dir / f'choropleth_{country.replace(" ", "_").lower()}_prefectures.html'
        m.save(str(output_file))
        logging.info(f"Choropleth map saved as {output_file}")

    def process_country(self, country, item_set_ids):
        logging.info(f"Processing data for {country}")
        
        try:
            all_items = []
            for item_set_id in item_set_ids:
                items = self.fetch_items(item_set_id)
                if items:
                    all_items.extend(items)
                    logging.info(f"Fetched {len(items)} items for item set {item_set_id}")

            if not all_items:
                logging.error(f"No items found for {country}")
                return

            coordinates = self.extract_coordinates(all_items)
            if not coordinates:
                logging.error(f"No coordinates extracted for {country}")
                return

            gdf = self.load_geojson(country)
            if gdf is None:
                return

            item_counts = self.count_items_per_prefecture(coordinates, gdf)
            if item_counts is None:
                return

            gdf = gdf.merge(item_counts, on='name', how='left')
            gdf['count'] = gdf['count'].fillna(0)

            self.generate_choropleth(gdf, country)
            logging.info(f"Successfully processed {country} with {len(all_items)} total items")
            
        except Exception as e:
            logging.error(f"Error processing {country}: {str(e)}", exc_info=True)

def main():
    processor = GeoDataProcessor()
    
    countries = {
        'Benin': [2185, 5502, 2186, 2188, 2187, 2191, 2190, 2189, 4922, 5501, 5500],
        'Burkina Faso': [2199, 2200, 23448, 23273, 23449, 5503, 2215, 2214, 2207, 2209, 2210, 2213, 2201],
        'Togo': [9458, 25304, 5498, 5499]
    }

    for country, item_set_ids in countries.items():
        processor.process_country(country, item_set_ids)

if __name__ == "__main__":
    main()