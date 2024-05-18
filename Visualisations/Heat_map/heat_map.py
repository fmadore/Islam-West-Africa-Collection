import os
import requests
from tqdm.auto import tqdm
import folium
from folium.plugins import HeatMap
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


def generate_heatmap(coordinates, country):
    if not coordinates:
        logging.info(f"No valid coordinates found for {country}, heatmap not generated.")
        return

    map_center = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude']).mean().to_list()
    m = folium.Map(location=map_center, zoom_start=2)  # Global view

    HeatMap(coordinates).add_to(m)

    html_file_path = f"heatmap_{country.replace(' ', '_').lower()}.html"
    m.save(html_file_path)
    logging.info(f"Heatmap saved as {html_file_path}")


def extract_and_plot(item_set_ids, country):
    all_coordinates = []
    for item_set_id in item_set_ids:
        items = fetch_items(item_set_id)
        if items:
            coordinates = extract_coordinates(items)
            all_coordinates.extend(coordinates)

    generate_heatmap(all_coordinates, country)


countries = {
    'Benin': [2185, 5502, 2186, 2187, 2188, 2189, 2190, 2191, 4922, 5501, 5500],
    'Burkina Faso': [2199, 2200, 2215, 2214, 2207, 2201, 23448, 5503, 2209, 2210, 2213],
    'Togo': [25304, 9458, 5498]
}

for country, item_set_ids in countries.items():
    logging.info(f"Processing data for {country}")
    extract_and_plot(item_set_ids, country)
