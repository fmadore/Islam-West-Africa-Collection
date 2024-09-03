import requests
import networkx as nx
from collections import defaultdict
from tqdm import tqdm
import csv
import os

base_url = "https://iwac.frederickmadore.com/api"

def get_imam_data(imam_id):
    response = requests.get(f"{base_url}/items/{imam_id}")
    return response.json()

def get_resource_data(resource_id):
    response = requests.get(f"{base_url}/resources/{resource_id}")
    return response.json()

def categorize_subject(subject_data):
    item_set = subject_data.get('o:item_set', [])
    if item_set and isinstance(item_set[0], dict):
        set_id = item_set[0].get('o:id')
        if set_id == 1:
            return 'Topic'
        elif set_id == 2:
            return 'Event'
        elif set_id == 266:
            return 'Person'
        elif set_id == 854:
            return 'Organization'
    return None

def extract_subjects(resource_data):
    subjects = defaultdict(list)
    for subject in resource_data.get('dcterms:subject', []):
        if isinstance(subject, dict) and '@id' in subject:
            subject_data = get_resource_data(subject['@id'].split('/')[-1])
            category = categorize_subject(subject_data)
            if category:
                subjects[category].append(subject_data.get('o:title', 'Unknown Subject'))
    return subjects

def extract_locations(resource_data):
    locations = []
    for location in resource_data.get('dcterms:spatial', []):
        if isinstance(location, dict) and '@id' in location:
            try:
                loc_data = get_resource_data(location['@id'].split('/')[-1])
                coords = next(
                    (item['@value'] for item in loc_data.get('curation:coordinates', []) if item['type'] == 'literal'),
                    None)
                locations.append({
                    'name': loc_data.get('o:title', 'Unknown Location'),
                    'coordinates': coords.split(', ') if coords else None
                })
            except Exception as e:
                print(f"Error processing location: {location}. Error: {str(e)}")
    return locations

def extract_date(resource_data):
    date_data = resource_data.get('dcterms:date', [])
    for date_item in date_data:
        if isinstance(date_item, dict) and date_item.get('type') == 'numeric:timestamp':
            return date_item.get('@value')
    return None

def extract_country(imam_data):
    for spatial in imam_data.get('dcterms:spatial', []):
        if isinstance(spatial, dict) and spatial.get('type') == 'resource:item':
            return spatial.get('display_title')
    return "Unknown"

def get_country_coordinates(country_id):
    country_data = get_resource_data(country_id)
    coordinates = country_data.get('curation:coordinates', [])
    for coord in coordinates:
        if isinstance(coord, dict) and coord.get('type') == 'literal':
            return coord.get('@value')
    return None

def create_network(imam_data):
    G = nx.Graph()

    for imam, data in imam_data.items():
        G.add_node(imam, type='Imam', country=data['country'])

        for category, subjects in data['subjects'].items():
            for subject in subjects:  # Remove set() to count all occurrences
                if not G.has_node(subject):
                    G.add_node(subject, type=category)
                if G.has_edge(imam, subject):
                    G[imam][subject]['weight'] += 1
                else:
                    G.add_edge(imam, subject, type=category, weight=1)

        for location in data['locations']:
            location_name = location['name']
            if not G.has_node(location_name):
                G.add_node(location_name, type='Location')
            if G.has_edge(imam, location_name):
                G[imam][location_name]['weight'] += 1
            else:
                G.add_edge(imam, location_name, type='Location', weight=1)

    return G

def export_gephi_files(G, nodes_file='gephi_nodes.csv', edges_file='gephi_edges.csv'):
    # Export nodes
    with open(nodes_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Id', 'Label', 'Type', 'Country']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writeheader()

        for node, data in G.nodes(data=True):
            writer.writerow({
                'Id': node,
                'Label': node,
                'Type': data.get('type', ''),
                'Country': data.get('country', '')
            })

    # Export edges with weight
    with open(edges_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Source', 'Target', 'Type', 'Weight']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writeheader()

        for source, target, data in G.edges(data=True):
            writer.writerow({
                'Source': source,
                'Target': target,
                'Type': data.get('type', ''),
                'Weight': data.get('weight', 1)
            })

# Main execution
imam_ids = [1124, 2150, 1615, 861, 945, 944, 855, 1940, 925]  # Add more imam IDs as needed

imam_data = {}
all_locations = []
all_subjects = defaultdict(list)

for imam_id in tqdm(imam_ids, desc="Processing imams"):
    try:
        imam = get_imam_data(imam_id)
        imam_name = imam['o:title']
        country = extract_country(imam)
        country_id = next((spatial['@id'].split('/')[-1] for spatial in imam.get('dcterms:spatial', [])
                           if isinstance(spatial, dict) and spatial.get('type') == 'resource:item'), None)
        country_coordinates = get_country_coordinates(country_id) if country_id else None

        imam_data[imam_name] = {
            'documents': [],
            'subjects': defaultdict(list),
            'locations': [],
            'country': country,
            'country_coordinates': country_coordinates
        }

        for doc in tqdm(imam['@reverse'].get('dcterms:subject', []), desc=f"Processing documents for {imam_name}",
                        leave=False):
            try:
                resource_id = doc['@id'].split('/')[-1]
                resource_data = get_resource_data(resource_id)

                doc_title = doc.get('o:title', 'Unknown Document')
                doc_date = extract_date(resource_data)
                imam_data[imam_name]['documents'].append({'title': doc_title, 'date': doc_date})

                subjects = extract_subjects(resource_data)
                for category, subject_list in subjects.items():
                    imam_data[imam_name]['subjects'][category].extend(subject_list)
                    all_subjects[category].extend(subject_list)

                locations = extract_locations(resource_data)
                imam_data[imam_name]['locations'].extend(locations)
                all_locations.extend(locations)
            except Exception as e:
                print(f"Error processing document for {imam_name}: {str(e)}")

        print(f"Successfully processed imam: {imam_name}")
    except Exception as e:
        print(f"Error processing imam with ID {imam_id}: {str(e)}")

print("\nCreating network based on imams, subjects, and locations...")
G = create_network(imam_data)

print("\nNetwork Analysis:")
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

if G.number_of_nodes() > 0:
    # Handle centrality measures for a single node
    if G.number_of_nodes() == 1:
        imam_name = list(G.nodes())[0]
        degree_cent = {imam_name: 0}
        eigenvector_cent = {imam_name: 1}
        betweenness_cent = {imam_name: 0}
    else:
        degree_cent = nx.degree_centrality(G)
        eigenvector_cent = nx.eigenvector_centrality(G, weight='weight')
        betweenness_cent = nx.betweenness_centrality(G, weight='weight')

    centrality_data = {
        imam: (degree_cent[imam], eigenvector_cent[imam], betweenness_cent[imam])
        for imam in imam_data.keys()
    }

    print("\nCentrality measures:")
    for imam, (deg, eig, bet) in centrality_data.items():
        print(f"{imam}: Degree={deg:.3f}, Eigenvector={eig:.3f}, Betweenness={bet:.3f}")
else:
    print("The graph is empty. Skipping centrality analysis.")

print("\nExporting data to CSV files...")

try:
    # Export data for Gephi
    export_gephi_files(G)
    print("Successfully created gephi_nodes.csv and gephi_edges.csv")
except Exception as e:
    print(f"Error creating Gephi files: {str(e)}")

print(f"\nCurrent working directory: {os.getcwd()}")
print("Script execution completed.")