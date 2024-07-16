import os
import requests
from tqdm.auto import tqdm
import folium
import geopandas as gpd
import pandas as pd
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

# Define API credentials
API_URL = "https://iwac.frederickmadore.com/api"
KEY_IDENTITY = os.getenv("API_KEY_IDENTITY")
KEY_CREDENTIAL = os.getenv("API_KEY_CREDENTIAL")

if not KEY_IDENTITY or not KEY_CREDENTIAL:
    logging.error("API_KEY_IDENTITY and API_KEY_CREDENTIAL must be set in the .env file.")
    exit(1)

def fetch_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed for URL {url}: {e}")
        return None

def fetch_items(item_set_id):
    items = []
    page = 1
    url = f"{API_URL}/items?key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}&item_set_id={item_set_id}&per_page=50&page={page}"

    while url:
        data = fetch_json(url)
        if data is None:
            break
        if isinstance(data, list):
            items.extend(data)
            break  # If the response is a list, assume no pagination
        elif isinstance(data, dict):
            items.extend(data.get('data', []))
            next_link = data.get('links', {}).get('next', {}).get('href')
            if next_link:
                url = f"{API_URL}{next_link}&key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}"
            else:
                url = None
        else:
            logging.error(f"Unexpected data format: {data}")
            break
    return items

def fetch_coordinates(spatial_url):
    spatial_details = fetch_json(spatial_url)
    if not spatial_details:
        return None

    coordinates_field = spatial_details.get('curation:coordinates', [])
    if coordinates_field:
        coord_text = coordinates_field[0].get('@value')
        if coord_text and ',' in coord_text:
            try:
                lat, lon = map(float, coord_text.split(','))
                return lat, lon
            except ValueError:
                logging.warning(f"Invalid coordinates format: {coord_text}")
                return None
    return None

def extract_coordinates(items):
    coordinates = []
    for item in tqdm(items, desc="Extracting coordinates"):
        spatial_data = item.get('dcterms:spatial', [])
        for spatial in spatial_data:
            spatial_url = spatial.get('@id')
            if spatial_url:
                coords = fetch_coordinates(spatial_url)
                if coords:
                    coordinates.append(coords)
    return coordinates


def load_geojson(country):
    # Create a valid filename by replacing spaces with underscores
    filename = f"{country.lower().replace(' ', '_')}_regions.geojson"
    geojson_path = os.path.join("data", filename)

    if not os.path.exists(geojson_path):
        logging.error(f"GeoJSON file not found: {geojson_path}")
        return None

    gdf = gpd.read_file(geojson_path)
    # Ensure the 'name' column is present
    if 'name' not in gdf.columns:
        gdf['name'] = gdf['properties'].apply(lambda x: x.get('name', 'Unknown'))
    return gdf


def count_items_per_region(coordinates, gdf):
    # Convert coordinates to GeoDataFrame
    points = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy([c[1] for c in coordinates], [c[0] for c in coordinates]),
        crs="EPSG:4326"
    )

    # Ensure the GeoDataFrame has the same CRS
    gdf = gdf.to_crs("EPSG:4326")

    # Perform spatial join
    joined = gpd.sjoin(points, gdf, how="inner", predicate="within")

    # Count items per region
    return joined.groupby('name').size().reset_index(name='count')


def generate_choropleth(gdf, country):
    # Calculate the center of the map using the bounds of the GeoDataFrame
    bounds = gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    # Create a base map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Add choropleth layer
    folium.Choropleth(
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

    # Add hover functionality
    folium.LayerControl().add_to(m)
    style_function = lambda x: {'fillColor': '#ffffff',
                                'color':'#000000',
                                'fillOpacity': 0.1,
                                'weight': 0.1}
    highlight_function = lambda x: {'fillColor': '#000000',
                                    'color':'#000000',
                                    'fillOpacity': 0.50,
                                    'weight': 0.1}
    info = folium.features.GeoJson(
        gdf,
        style_function=style_function,
        control=False,
        highlight_function=highlight_function,
        tooltip=folium.features.GeoJsonTooltip(
            fields=['name', 'count'],
            aliases=['Region', 'Item Count'],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
        )
    )
    m.add_child(info)
    m.keep_in_front(info)

    # Save the map
    html_file_path = f"choropleth_{country.replace(' ', '_').lower()}.html"
    m.save(html_file_path)
    logging.info(f"Choropleth map saved as {html_file_path}")


def extract_and_plot(item_set_ids, country):
    all_coordinates = []
    total_items = 0

    for item_set_id in item_set_ids:
        items = fetch_items(item_set_id)
        if items:
            total_items += len(items)
            coordinates = extract_coordinates(items)
            all_coordinates.extend(coordinates)

    gdf = load_geojson(country)
    if gdf is None:
        logging.error(f"Failed to load GeoJSON for {country}. Skipping.")
        return

    item_counts = count_items_per_region(all_coordinates, gdf)

    # Merge item counts with GeoDataFrame
    gdf = gdf.merge(item_counts, on='name', how="left")
    gdf['count'] = gdf['count'].fillna(0)

    generate_choropleth(gdf, country)
    logging.info(f"Total items processed for {country}: {total_items}")


countries = {
    'Benin': [2185],
    'Burkina Faso': [2199],
    'Togo': [25304]
}

for country, item_set_ids in countries.items():
    logging.info(f"Processing data for {country}")
    extract_and_plot(item_set_ids, country)