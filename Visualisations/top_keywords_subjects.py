import requests
import pandas as pd
import plotly.graph_objs as go
from tqdm.auto import tqdm
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed


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
def fetch_and_process_data(api_url, item_sets):
    all_items = []
    # Use ThreadPoolExecutor to parallelize requests
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(fetch_data, api_url, id): id for id in item_sets}
        for future in as_completed(future_to_id):
            all_items.extend(future.result())

    # Process items to extract subjects and date
    processed_data = []
    for item in all_items:
        subjects = [sub['display_title'] for sub in item.get('dcterms:subject', []) if sub.get('display_title')]
        date = item.get('dcterms:date', [{}])[0].get('@value')
        for subject in subjects:
            processed_data.append({
                'Subject': subject,
                'Date': pd.to_datetime(date, errors='coerce')
            })

    return pd.DataFrame(processed_data)


# Function to create an interactive keyword graph for each country
def create_interactive_keyword_graph(df, country, output_filename):
    top_keywords = Counter(df['Subject']).most_common(10)
    top_keywords = [keyword for keyword, count in top_keywords]

    df_top_keywords = df[df['Subject'].isin(top_keywords)]
    df_grouped = df_top_keywords.groupby([df_top_keywords['Date'].dt.year, 'Subject']).size().reset_index(
        name='Frequency')

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
        title=f"Annual Frequency of Top 10 Keywords in {country}",
        xaxis=dict(title="Year", rangeslider=dict(visible=True), type="date"),
        yaxis=dict(title="Frequency")
    )

    fig.write_html(f"{output_filename}_{country}.html", full_html=True, include_plotlyjs='cdn')


# Example usage
api_url = "https://iwac.frederickmadore.com/api"
country_item_sets = {
    "Bénin": ["2187", "2188", "2189"],
    "Burkina Faso": ["2189"]
}

# Process and create graphs for each country
for country, item_sets in country_item_sets.items():
    df = fetch_and_process_data(api_url, item_sets)
    create_interactive_keyword_graph(df, country, "top_keywords_graph")
    tqdm.write(f"Interactive graph has been created for {country}.")
