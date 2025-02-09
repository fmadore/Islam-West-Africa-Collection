import os
import requests
from tqdm.auto import tqdm
import folium
import geopandas as gpd
import pandas as pd
import logging
from dotenv import load_dotenv
from pyproj import CRS, Transformer
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

# Define API credentials
API_URL = "https://islam.zmo.de/api"
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
            logging.error(f"Failed to fetch data for item set {item_set_id} on page {page}")
            break

        if isinstance(data, list):
            items.extend(data)
            logging.info(f"Fetched {len(data)} items for item set {item_set_id} (single page response)")
            if len(data) < per_page:
                break  # Last page
        elif isinstance(data, dict):
            page_items = data.get('data', [])
            items.extend(page_items)
            logging.info(f"Fetched {len(page_items)} items for item set {item_set_id} (page {page})")
            if 'next' not in data.get('links', {}):
                break  # No more pages
        else:
            logging.error(f"Unexpected data format for item set {item_set_id} on page {page}: {type(data)}")
            break

        page += 1

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
    spatial_coverage_count = Counter()
    items_with_spatial_data = 0

    # First pass: count occurrences of each spatial coverage value
    for item in tqdm(items, desc="Counting spatial coverage occurrences"):
        spatial_data = item.get('dcterms:spatial', [])
        if spatial_data:
            items_with_spatial_data += 1
            for spatial in spatial_data:
                spatial_url = spatial.get('@id')
                if spatial_url:
                    spatial_coverage_count[spatial_url] += 1
                else:
                    logging.warning(f"Spatial data in item {item.get('@id', 'Unknown')} has no '@id' field")

    logging.info(f"Found {len(spatial_coverage_count)} unique spatial coverage values")
    logging.info(f"{items_with_spatial_data} out of {len(items)} items have spatial data")

    # Second pass: fetch coordinates for unique spatial coverage values
    coordinates = []
    unique_coords = {}
    for spatial_url, count in tqdm(spatial_coverage_count.items(), desc="Fetching unique coordinates"):
        coords = fetch_coordinates(spatial_url)
        if coords:
            unique_coords[spatial_url] = coords
            coordinates.extend([coords] * count)
        else:
            logging.warning(f"No valid coordinates found for spatial coverage: {spatial_url}")

    logging.info(f"Fetched coordinates for {len(unique_coords)} out of {len(spatial_coverage_count)} unique spatial coverage values")
    logging.info(f"Total coordinates extracted: {len(coordinates)}")

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

def load_global_boundaries():
    boundaries_path = os.path.join("data", "world_countries.geojson")
    if not os.path.exists(boundaries_path):
        logging.error(f"Global boundaries GeoJSON file not found: {boundaries_path}")
        return None
    return gpd.read_file(boundaries_path)


def count_items_per_region(coordinates, gdf, world_gdf, country):
    points = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy([c[1] for c in coordinates], [c[0] for c in coordinates]),
        crs="EPSG:4326"
    )

    gdf = gdf.to_crs("EPSG:4326")
    world_gdf = world_gdf.to_crs("EPSG:4326")

    # Count items within the country's regions
    joined = gpd.sjoin(points, gdf, how="inner", predicate="within")
    region_counts = joined.groupby('name').size().reset_index(name='count')

    # Count items in all countries
    world_joined = gpd.sjoin(points, world_gdf, how="inner", predicate="within")
    world_counts = world_joined.groupby('name').size().reset_index(name='count')

    # Separate foreign countries
    foreign_counts = world_counts[world_counts['name'] != country].copy()
    foreign_counts['name'] = foreign_counts['name'] + ' (Foreign)'

    # Combine counts
    all_counts = pd.concat([region_counts, foreign_counts])

    return all_counts


def generate_choropleth(gdf, world_gdf, country, all_counts):
    # Set the map center to the country of focus
    country_gdf = world_gdf[world_gdf['name'] == country]
    if not country_gdf.empty:
        # Project to a suitable CRS for centroid calculation
        projected_gdf = country_gdf.to_crs(CRS.from_epsg(3857))  # Web Mercator projection
        centroid = projected_gdf.geometry.centroid.iloc[0]
        # Convert back to WGS84 for Folium
        transformer = Transformer.from_crs(CRS.from_epsg(3857), CRS.from_epsg(4326), always_xy=True)
        center_lon, center_lat = transformer.transform(centroid.x, centroid.y)
    else:
        center_lat, center_lon = gdf.to_crs(CRS.from_epsg(4326)).total_bounds[[1, 0]]

    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Prepare data for choropleth layers
    world_counts = all_counts[all_counts['name'] != country]
    world_gdf = world_gdf.merge(world_counts, on='name', how='left')
    world_gdf['count'] = world_gdf['count'].fillna(0)

    # Ensure 'count' column exists in gdf
    if 'count' not in gdf.columns:
        gdf = gdf.merge(all_counts[all_counts['name'] == country], on='name', how='left')
        gdf['count'] = gdf['count'].fillna(0)

    # Add choropleth layer for global data
    if 'count' in world_gdf.columns:
        folium.Choropleth(
            geo_data=world_gdf.__geo_interface__,
            name="Global Data",
            data=world_gdf,
            columns=['name', 'count'],
            key_on='feature.properties.name',
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Global Item Count"
        ).add_to(m)

    # Add choropleth layer for country regions
    if 'count' in gdf.columns:
        folium.Choropleth(
            geo_data=gdf.__geo_interface__,
            name="Country Regions",
            data=gdf,
            columns=['name', 'count'],
            key_on='feature.properties.name',
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Region Item Count"
        ).add_to(m)

    # Add hover functionality for global data
    folium.GeoJson(
        world_gdf,
        name="Global Data",
        style_function=lambda x: {'fillColor': 'transparent', 'color': 'transparent'},
        highlight_function=lambda x: {'fillColor': '#000000', 'color': '#000000', 'fillOpacity': 0.50, 'weight': 0.1},
        tooltip=folium.features.GeoJsonTooltip(
            fields=['name', 'count'],
            aliases=['Country', 'Item Count'],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
        )
    ).add_to(m)

    # Add hover functionality for country regions
    folium.GeoJson(
        gdf,
        name="Country Regions",
        style_function=lambda x: {'fillColor': 'transparent', 'color': 'transparent'},
        highlight_function=lambda x: {'fillColor': '#000000', 'color': '#000000', 'fillOpacity': 0.50, 'weight': 0.1},
        tooltip=folium.features.GeoJsonTooltip(
            fields=['name', 'count'],
            aliases=['Region', 'Item Count'],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
        )
    ).add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    html_file_path = f"choropleth_{country.replace(' ', '_').lower()}_regions_and_countries.html"
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
    world_gdf = load_global_boundaries()
    if gdf is None or world_gdf is None:
        logging.error(f"Failed to load GeoJSON for {country} or global boundaries. Skipping.")
        return

    all_counts = count_items_per_region(all_coordinates, gdf, world_gdf, country)

    # Remove '(Foreign)' suffix from country names
    all_counts['name'] = all_counts['name'].str.replace(' (Foreign)', '')

    # Ensure 'count' column exists in gdf
    if 'count' not in gdf.columns:
        gdf = gdf.merge(all_counts[all_counts['name'] != country], on='name', how='left')
        gdf['count'] = gdf['count'].fillna(0)

    generate_choropleth(gdf, world_gdf, country, all_counts)
    logging.info(f"Total items processed for {country}: {total_items}")


countries = {
    'Benin': [2185, 5502, 2186, 2188, 2187, 2191, 2190, 2189, 4922, 5501, 5500],
    'Burkina Faso': [2199, 2200, 23448, 23273, 23449, 5503, 2215, 2214, 2207, 2209, 2210, 2213, 2201],
    'Togo': [9458, 25304, 5498, 5499]
}

for country, item_set_ids in countries.items():
    logging.info(f"Processing data for {country}")
    extract_and_plot(item_set_ids, country)