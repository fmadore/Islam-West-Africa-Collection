import requests
from tqdm.auto import tqdm
import plotly.express as px
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


def extract_and_plot(item_set_id, country):
    items = fetch_items(item_set_id)
    all_coordinates = []

    pbar = tqdm(total=len(items), desc=f"Processing {country}")

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
        df = pd.DataFrame(all_coordinates, columns=['Latitude', 'Longitude'])

        # Set the zoom and center for a global view
        fig = px.density_mapbox(df, lat='Latitude', lon='Longitude', radius=10,
                                center=dict(lat=0, lon=0), zoom=1,  # zoom set for a global view
                                mapbox_style="stamen-terrain")
        fig.update_layout(title=f"Heatmap of {country}", margin={"r": 0, "t": 0, "l": 0, "b": 0})
        html_file_path = f"{country.replace(' ', '_').lower()}_heatmap.html"
        fig.write_html(html_file_path)
        print(f"Heatmap saved as {html_file_path}")
    else:
        print(f"No valid coordinates found for {country}, heatmap not generated.")


countries = {
    'Benin': 2188,
    'Burkina Faso': 2200
}

for country, item_set_id in countries.items():
    extract_and_plot(item_set_id, country)
