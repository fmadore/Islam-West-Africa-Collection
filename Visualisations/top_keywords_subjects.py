import requests
import pandas as pd
import plotly.graph_objs as go
from tqdm.auto import tqdm
from collections import Counter

# Function to fetch data from the API
def fetch_and_process_data(api_url, item_set_ids):
    all_items = []
    tqdm.write("Starting to fetch data from the API...")
    for item_set_id in item_set_ids:
        page = 1
        with tqdm(desc=f"Fetching item set {item_set_id}") as pbar:
            while True:
                response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page})
                data = response.json()
                if data:
                    all_items.extend(data)
                    pbar.update(1)
                    page += 1
                else:
                    break
            pbar.close()
    tqdm.write(f"Finished fetching data for all item sets.")

    # Process items to extract subjects and date
    processed_data = []
    for item in tqdm(all_items, desc="Processing items"):
        subjects = [sub['display_title'] for sub in item.get('dcterms:subject', []) if sub.get('display_title')]
        date = item.get('dcterms:date', [{}])[0].get('@value')
        for subject in subjects:
            processed_data.append({
                'Subject': subject,
                'Date': pd.to_datetime(date, errors='coerce')
            })

    return pd.DataFrame(processed_data)

# Function to create an interactive keyword graph
def create_interactive_keyword_graph(df, output_filename):
    # Find the 10 most prominent keywords across all years
    top_keywords = Counter(df['Subject']).most_common(10)
    top_keywords = [keyword for keyword, count in top_keywords]

    # Filter dataframe to only include top keywords
    df_top_keywords = df[df['Subject'].isin(top_keywords)]

    # Group by year and subject to get counts
    df_grouped = df_top_keywords.groupby([df_top_keywords['Date'].dt.year, 'Subject']).size().reset_index(name='Frequency')

    # Create the base figure
    fig = go.Figure()

    # Add traces for each keyword
    for keyword in top_keywords:
        df_keyword = df_grouped[df_grouped['Subject'] == keyword]
        fig.add_trace(go.Scatter(
            x=df_keyword['Date'],
            y=df_keyword['Frequency'],
            mode='lines+markers',
            name=keyword
        ))

    # Update the layout to add a year slider
    fig.update_layout(
        title="Annual Frequency of Top 10 Keywords",
        xaxis=dict(
            title="Year",
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1yr", step="year", stepmode="backward"),
                    dict(count=3, label="3yrs", step="year", stepmode="backward"),
                    dict(count=5, label="5yrs", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(visible=True),
            type="date"
        ),
        yaxis=dict(title="Frequency")
    )

    # Write the plot to an HTML file
    fig.write_html(output_filename, full_html=True, include_plotlyjs='cdn')

# Set the API URL and item set IDs
api_url = "https://iwac.frederickmadore.com/api"
item_sets = ["2187", "2207"]

# Fetch and process the data
df = fetch_and_process_data(api_url, item_sets)

# Define the output filename for the HTML
output_html = "top_keywords_graph.html"

# Create the interactive graph
create_interactive_keyword_graph(df, output_html)

tqdm.write("Interactive graph has been created.")
