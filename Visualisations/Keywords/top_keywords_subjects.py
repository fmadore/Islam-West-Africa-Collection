import os
import requests
import pandas as pd
import plotly.graph_objs as go
import logging
from tqdm.auto import tqdm
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to fetch data from a single item set
def fetch_data(api_url, item_set_id):
    page = 1
    items = []
    while True:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page})
        data = response.json()
        if data:
            items.extend(data)
            page += 1
        else:
            break
    return items

# Function to fetch and process data for all item sets in a country
def fetch_and_process_data(api_url, item_sets, country):
    logger.info(f"Fetching and processing data for {country}")
    all_items = []
    with tqdm(total=len(item_sets), desc=f"Fetching data for {country}", unit="item set") as pbar:
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_id = {executor.submit(fetch_data, api_url, id): id for id in item_sets}
            for future in as_completed(future_to_id):
                all_items.extend(future.result())
                pbar.update(1)

    logger.info(f"Processing {len(all_items)} items for {country}")
    processed_data = []
    for item in tqdm(all_items, desc=f"Processing items for {country}", unit="item"):
        subjects = [sub['display_title'] for sub in item.get('dcterms:subject', []) if sub.get('display_title')]
        date = item.get('dcterms:date', [{}])[0].get('@value')
        for subject in subjects:
            processed_data.append({
                'Subject': subject,
                'Date': pd.to_datetime(date, errors='coerce')
            })

    df = pd.DataFrame(processed_data)
    logger.info(f"Processed {len(df)} data points for {country}")
    return df

# Function to create interactive keyword graphs for each country in English and French
def create_interactive_keyword_graphs(df, country, output_filename):
    logger.info(f"Creating interactive keyword graphs for {country} in English and French")
    top_keywords = Counter(df['Subject']).most_common(10)
    top_keywords = [keyword for keyword, count in top_keywords]

    df_top_keywords = df[df['Subject'].isin(top_keywords)]
    df_grouped = df_top_keywords.groupby([df_top_keywords['Date'].dt.year, 'Subject']).size().reset_index(
        name='Frequency')

    # Define titles and labels in English and French
    titles = {
        'en': f"Annual Frequency of Top 10 Keywords ({country})",
        'fr': f"Fréquence annuelle des 10 mots-clés les plus fréquents ({country})"
    }
    x_labels = {'en': "Year", 'fr': "Année"}
    y_labels = {'en': "Frequency", 'fr': "Fréquence"}

    for lang in ['en', 'fr']:
        fig = go.Figure()
        for keyword in top_keywords:
            df_keyword = df_grouped[df_grouped['Subject'] == keyword]
            fig.add_trace(go.Scatter(
                x=df_keyword['Date'],
                y=df_keyword['Frequency'],
                mode='lines+markers',
                name=keyword
            ))

        fig.update_layout(
            title=titles[lang],
            xaxis=dict(title=x_labels[lang], rangeslider=dict(visible=True), type="date"),
            yaxis=dict(title=y_labels[lang])
        )

        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, f"{output_filename}_{country}_{lang}.html")
        
        fig.write_html(output_path, full_html=True, include_plotlyjs='cdn')
        logger.info(f"Interactive graph for {country} in {lang} saved to {output_path}")

# Example usage
api_url = "https://iwac.frederickmadore.com/api"
country_item_sets = {
    "Bénin": ["2187", "2188", "2189"],
    "Burkina Faso": ["2200", "2215", "2214", "2207", "2201"],
    "Togo": ["5498", "5499"]
}

# Process and create graphs for each country
for country, item_sets in country_item_sets.items():
    df = fetch_and_process_data(api_url, item_sets, country)
    create_interactive_keyword_graphs(df, country, "top_keywords_graph")
    tqdm.write(f"Interactive graph has been created for {country}.")
