import requests
from tqdm.auto import tqdm
import folium
from folium.plugins import HeatMap
import pandas as pd


def fetch_json(url):
    response = requests.get(url)
    return response.json()


def fetch_items(item_set_id):
    items = []
    url = f"https://iwac.frederickmadore.com/api/items?item_set_id={item_set_id}&per_page=50"
    while url:
        response = requests.get(url)
        data = response.json()
        items.extend(data)
        url = response.links.get('next', {}).get('url') if 'next' in response.links else None
    return items


def fetch_coordinates(spatial_url):
    # Fetching item details directly using the provided spatial URL
    spatial_details = fetch_json(spatial_url)
    coordinates_field = spatial_details.get('curation:coordinates', [])
    if coordinates_field:
        coord_text = coordinates_field[0].get('@value')
        if coord_text and ',' in coord_text:
            try:
                lat, lon = map(float, coord_text.split(','))
                return lat, lon
            except ValueError:
                print(f"Invalid coordinates format: {coord_text}")
                return None
    return None


def extract_and_plot(item_set_ids, country):
    all_coordinates = []

    for item_set_id in item_set_ids:
        items = fetch_items(item_set_id)  # Fetch items first
        pbar = tqdm(total=len(items), desc=f"Processing {country}: Item Set {item_set_id}")

        for item in items:
            spatial_data = item.get('dcterms:spatial', [])
            for spatial in spatial_data:
                spatial_url = spatial.get('@id')
                if spatial_url:
                    coords = fetch_coordinates(spatial_url)
                    if coords:
                        all_coordinates.append(coords)
            pbar.update(1)
        pbar.close()

    if all_coordinates:
        map_center = pd.DataFrame(all_coordinates, columns=['Latitude', 'Longitude']).mean().to_list()
        m = folium.Map(location=map_center, zoom_start=2)  # Global view

        HeatMap(all_coordinates).add_to(m)

        html_file_path = f"{country.replace(' ', '_').lower()}_heatmap.html"
        m.save(html_file_path)
        print(f"Heatmap saved as {html_file_path}")
    else:
        print(f"No valid coordinates found for {country}, heatmap not generated.")


countries = {
    'Benin': [2187, 2188, 2189],
    'Burkina Faso': [2200, 2215, 2214, 2207, 2201]
}

for country, item_set_ids in countries.items():
    extract_and_plot(item_set_ids, country)
