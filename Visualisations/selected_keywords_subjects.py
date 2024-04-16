import requests
import pandas as pd
import plotly.graph_objs as go
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def fetch_title_for_id(api_url, keyword_id):
    response = requests.get(f"{api_url}/items/{keyword_id}")
    data = response.json()
    return data.get('dcterms:title', [{}])[0].get('@value', 'Unknown Title')

def fetch_and_process_data(api_url, item_sets, selected_keyword_ids):
    all_items = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(fetch_data, api_url, id): id for id in item_sets}
        for future in tqdm(as_completed(future_to_id), total=len(item_sets), desc="Fetching item sets"):
            all_items.extend(future.result())

    processed_data = []
    selected_keyword_ids_set = set(map(int, selected_keyword_ids))  # Convert to set of integers for faster lookup
    for item in tqdm(all_items, desc="Processing items"):
        subjects = item.get('dcterms:subject', [])
        date = item.get('dcterms:date', [{}])[0].get('@value')
        for subject in subjects:
            if subject.get('value_resource_id') in selected_keyword_ids_set:
                processed_data.append({
                    'Subject': subject['display_title'],
                    'Date': pd.to_datetime(date, errors='coerce'),
                    'ID': subject['value_resource_id']
                })

    return pd.DataFrame(processed_data)

def create_interactive_keyword_graph(api_url, df, selected_keyword_ids, output_filename):
    if df.empty:
        print("No data available for the selected keyword IDs.")
        return

    # Fetch titles for the keywords
    keyword_titles = {str(id): fetch_title_for_id(api_url, id) for id in tqdm(selected_keyword_ids, desc="Fetching titles")}

    df_grouped = df.groupby([df['Date'].dt.year, 'Subject', 'ID']).size().reset_index(name='Frequency')

    fig = go.Figure()
    for keyword_id in selected_keyword_ids:
        if keyword_id in df['ID'].astype(str).unique():
            subject_title = keyword_titles[keyword_id]
            df_keyword = df_grouped[df_grouped['ID'] == int(keyword_id)]
            fig.add_trace(go.Scatter(
                x=df_keyword['Date'],
                y=df_keyword['Frequency'],
                mode='lines+markers',
                name=subject_title  # Use the fetched title as the trace name
            ))
        else:
            print(f"No data found for ID {keyword_id}. Skipping this ID.")

    fig.update_layout(
        title="Annual Frequency of Selected Keywords",
        xaxis=dict(title="Year", rangeslider=dict(visible=True), type="date"),
        yaxis=dict(title="Frequency"),
        legend_title="Keyword Title"
    )

    fig.write_html(f"{output_filename}.html", full_html=True, include_plotlyjs='cdn')
    print(f"Interactive graph has been created. File saved as '{output_filename}.html'")

# Example usage
api_url = "https://iwac.frederickmadore.com/api"
all_item_sets = ["2188"]
selected_keyword_ids = ["29", "124"] # Enter keywords ID

df = fetch_and_process_data(api_url, all_item_sets, selected_keyword_ids)
create_interactive_keyword_graph(api_url, df, selected_keyword_ids, "selected_keywords_graph")
