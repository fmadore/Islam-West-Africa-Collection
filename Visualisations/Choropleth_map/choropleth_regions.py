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
    per_page = 100  # Increased from 50 to 100 for efficiency

    while True:
        url = f"{API_URL}/items?key_identity={KEY_IDENTITY}&key_credential={KEY_CREDENTIAL}&item_set_id={item_set_id}&per_page={per_page}&page={page}"
        data = fetch_json(url)

        if data is None:
            break

        if isinstance(data, list):
            items.extend(data)
            if len(data) < per_page:
                break  # Last page
        elif isinstance(data, dict):
            items.extend(data.get('data', []))
            if 'next' not in data.get('links', {}):
                break  # No more pages
        else:
            logging.error(f"Unexpected data format: {data}")
            break

        page += 1
        logging.info(f"Fetched page {page - 1} for item set {item_set_id}")

    logging.info(f"Total items fetched for item set {item_set_id}: {len(items)}")
    return items

def fetch_coordinates(spatial_url):
    spatial_details = fetch_json(spatial_url)
    if not spatial_details:
        logging.warning(f"Failed to fetch spatial details from {spatial_url}")
        return None

    coordinates_field = spatial_details.get('curation:coordinates', [])
    if not coordinates_field:
        logging.warning(f"No 'curation:coordinates' field found in spatial details from {spatial_url}")
        return None

    coord_text = coordinates_field[0].get('@value')
    if not coord_text or ',' not in coord_text:
        logging.warning(f"Invalid coordinate format in spatial details from {spatial_url}")
        return None

    try:
        lat, lon = map(float, coord_text.split(','))
        return lat, lon
    except ValueError:
        logging.warning(f"Invalid coordinate values in spatial details from {spatial_url}")
        return None


def extract_coordinates(items):
    coordinates = []
    for item in tqdm(items, desc="Extracting coordinates"):
        spatial_data = item.get('dcterms:spatial', [])
        if not spatial_data:
            logging.warning(f"Item {item.get('@id', 'Unknown')} has no spatial data")
            continue

        item_coordinates = []
        for spatial in spatial_data:
            spatial_url = spatial.get('@id')
            if spatial_url:
                coords = fetch_coordinates(spatial_url)
                if coords:
                    item_coordinates.append(coords)
            else:
                logging.warning(f"Spatial data in item {item.get('@id', 'Unknown')} has no '@id' field")

        if item_coordinates:
            coordinates.extend(item_coordinates)
        else:
            logging.warning(f"No valid coordinates found for item {item.get('@id', 'Unknown')}")

    logging.info(f"Extracted coordinates for {len(coordinates)} locations")
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
    html_file_path = f"choropleth_{country.replace(' ', '_').lower()}_regions.html"
    m.save(html_file_path)
    logging.info(f"Choropleth map saved as {html_file_path}")


def extract_and_plot(item_set_ids, country):
    all_coordinates = []
    total_items = 0

    for item_set_id in item_set_ids:
        items = fetch_items(item_set_id)
        if items:
            total_items += len(items)
            logging.info(f"Fetched {len(items)} items for item set {item_set_id}")
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
    'Benin': [2185, 5502, 2186, 2188, 2187, 2191, 2190, 2189, 4922, 5501, 5500],
    'Burkina Faso': [2199, 2200, 23448, 23273, 23449, 5503, 2215, 2214, 2207, 2209, 2210, 2213, 2201],
    'Togo': [9458, 25304, 5498, 5499]
}

for country, item_set_ids in countries.items():
    logging.info(f"Processing data for {country}")
    extract_and_plot(item_set_ids, country)