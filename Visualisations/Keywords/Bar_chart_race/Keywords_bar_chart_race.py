import os
import requests
import pandas as pd
import json
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

# Function to prepare data for Bar Chart Race
def prepare_bar_chart_race_data(df, country):
    logger.info(f"Preparing Bar Chart Race data for {country}")
    
    # Group by year and subject, count occurrences
    df_grouped = df.groupby([df['Date'].dt.year, 'Subject']).size().reset_index(name='Count')
    
    # Sort by year to ensure chronological order
    df_grouped = df_grouped.sort_values('Date')
    
    # Calculate cumulative occurrences for each subject
    cumulative_counts = df_grouped.groupby('Subject')['Count'].cumsum().rename('CumulativeCount')
    df_grouped['CumulativeCount'] = cumulative_counts
    
    # Prepare data in the format needed for D3.js Bar Chart Race
    data = []
    all_subjects = set()
    
    for year, group in df_grouped.groupby('Date'):
        year_data = group.set_index('Subject')[['CumulativeCount']].rename(columns={'CumulativeCount': 'value'})
        year_data = year_data.reset_index()
        year_data.columns = ['name', 'value']
        year_data = year_data.sort_values('value', ascending=False)
        
        # Keep only top 10 for this year
        year_data = year_data.head(10)
        
        all_subjects.update(year_data['name'])
        
        data.append({
            'year': int(year),
            'data': year_data.to_dict('records')
        })
    
    return data, list(all_subjects)

# Function to save data as JSON for D3.js
def save_bar_chart_race_data(data, country, output_filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, f"{output_filename}_{country}.json")
    
    with open(output_path, 'w') as f:
        json.dump(data, f)
    
    logger.info(f"Bar Chart Race data for {country} saved to {output_path}")

# Example usage
api_url = "https://iwac.frederickmadore.com/api"
country_item_sets = {
    "Bénin": ["2187", "2188", "2189"],
    "Burkina Faso": ["2200", "2215", "2214", "2207", "2201"],
    "Togo": ["5498", "5499"]
}

# Process and create Bar Chart Race data for each country
for country, item_sets in country_item_sets.items():
    df = fetch_and_process_data(api_url, item_sets, country)
    bar_chart_race_data, top_10_subjects = prepare_bar_chart_race_data(df, country)
    
    # Save the data and top 10 subjects
    output_data = {
        'data': bar_chart_race_data,
        'top_10_subjects': top_10_subjects
    }
    save_bar_chart_race_data(output_data, country, "bar_chart_race_data")
    tqdm.write(f"Bar Chart Race data has been created for {country}.")