import requests
import networkx as nx
from collections import Counter, defaultdict
from tqdm import tqdm
import csv
from datetime import datetime
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


def clean_text(text):
    """Clean text by replacing problematic characters with HTML entities."""
    return text.replace(',', '&#44;').replace(';', '&#59;').replace('-', '&#45;').replace('/', '&#47;')

def format_date(date_str):
    """Format date string, adding day if missing."""
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date.strftime("%Y-%m-%d")
    except ValueError:
        try:
            date = datetime.strptime(date_str, "%Y-%m")
            return date.strftime("%Y-%m-01")
        except ValueError:
            return date_str  # Return original if parsing fails


def export_combined_palladio_data(imam_data, G, degree_cent, eigenvector_cent, betweenness_cent, filename='palladio_data.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Type', 'Id', 'Label', 'Country', 'Latitude', 'Longitude', 'Degree Centrality',
                      'Eigenvector Centrality', 'Betweenness Centrality', 'Document Count',
                      'Unique Subjects', 'Unique Locations', 'Source', 'Target', 'Weight',
                      'Shared Subjects', 'Shared Locations', 'Subject', 'Subject Category',
                      'Subject Frequency', 'Location', 'Location Frequency', 'Date', 'Document Titles']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writeheader()

        # Write imam data
        for imam, data in imam_data.items():
            writer.writerow({
                'Type': 'Imam',
                'Id': imam,
                'Label': imam,
                'Country': data['country'],
                'Latitude': data['country_coordinates'].split(', ')[0] if data['country_coordinates'] else '',
                'Longitude': data['country_coordinates'].split(', ')[1] if data['country_coordinates'] else '',
                'Degree Centrality': f"{degree_cent[imam]:.6f}",
                'Eigenvector Centrality': f"{eigenvector_cent[imam]:.6f}",
                'Betweenness Centrality': f"{betweenness_cent[imam]:.6f}",
                'Document Count': len(data['documents']),
                'Unique Subjects': sum(len(set(subjects)) for subjects in data['subjects'].values()),
                'Unique Locations': len(set(loc['name'] for loc in data['locations']))
            })

        # Write edge data
        for edge in G.edges(data=True):
            writer.writerow({
                'Type': 'Edge',
                'Source': edge[0],
                'Target': edge[1],
                'Weight': edge[2]['weight'],
                'Shared Subjects': edge[2]['shared_subjects'],
                'Shared Locations': edge[2]['shared_locations']
            })

        # Write subject data
        for imam, data in imam_data.items():
            for category, subjects in data['subjects'].items():
                subject_counts = Counter(subjects)
                for subject, count in subject_counts.items():
                    writer.writerow({
                        'Type': 'Subject',
                        'Id': imam,
                        'Subject': subject,
                        'Subject Category': category,
                        'Subject Frequency': count
                    })

        # Write location data
        for imam, data in imam_data.items():
            location_counts = Counter(loc['name'] for loc in data['locations'])
            for location in data['locations']:
                if location['coordinates']:
                    writer.writerow({
                        'Type': 'Location',
                        'Id': imam,
                        'Location': location['name'],
                        'Latitude': location['coordinates'][0],
                        'Longitude': location['coordinates'][1],
                        'Location Frequency': location_counts[location['name']]
                    })

        # Write timeline data
        for imam, data in imam_data.items():
            date_docs = defaultdict(list)
            for doc in data['documents']:
                if doc['date']:
                    formatted_date = format_date(doc['date'])
                    date_docs[formatted_date].append(doc['title'])

            for date, docs in date_docs.items():
                writer.writerow({
                    'Type': 'Timeline',
                    'Id': imam,
                    'Date': date,
                    'Document Count': len(docs),
                    'Document Titles': '; '.join(docs)
                })

def export_gephi_files(imam_data, G, degree_cent, eigenvector_cent, betweenness_cent,
                       nodes_file='gephi_nodes.csv', edges_file='gephi_edges.csv'):
    # Export nodes
    with open(nodes_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Id', 'Label', 'Country', 'Latitude', 'Longitude', 'Degree Centrality',
                      'Eigenvector Centrality', 'Betweenness Centrality', 'Document Count',
                      'Unique Subjects', 'Unique Locations']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writeheader()

        for imam, data in imam_data.items():
            writer.writerow({
                'Id': imam,
                'Label': imam,
                'Country': data['country'],
                'Latitude': data['country_coordinates'].split(', ')[0] if data['country_coordinates'] else '',
                'Longitude': data['country_coordinates'].split(', ')[1] if data['country_coordinates'] else '',
                'Degree Centrality': f"{degree_cent[imam]:.6f}",
                'Eigenvector Centrality': f"{eigenvector_cent[imam]:.6f}",
                'Betweenness Centrality': f"{betweenness_cent[imam]:.6f}",
                'Document Count': len(data['documents']),
                'Unique Subjects': sum(len(set(subjects)) for subjects in data['subjects'].values()),
                'Unique Locations': len(set(loc['name'] for loc in data['locations']))
            })

    # Export edges
    with open(edges_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Source', 'Target', 'Weight', 'Shared Subjects', 'Shared Locations']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writeheader()

        for edge in G.edges(data=True):
            writer.writerow({
                'Source': edge[0],
                'Target': edge[1],
                'Weight': edge[2]['weight'],
                'Shared Subjects': edge[2]['shared_subjects'],
                'Shared Locations': edge[2]['shared_locations']
            })

# Main execution
imam_ids = [1124, 2150, 1615, 861, 945, 944, 855, 1940, 925]  # Add more imam IDs as needed

G = nx.Graph()
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

        G.add_node(imam_name, country=country)
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

print("\nNetwork Analysis:")
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

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

print("\nExporting data to CSV files...")

try:
    # Export combined data for Palladio
    export_combined_palladio_data(imam_data, G, degree_cent, eigenvector_cent, betweenness_cent)
    print("Successfully created palladio_data.csv")
except Exception as e:
    print(f"Error creating palladio_data.csv: {str(e)}")

try:
    # Export data for Gephi
    export_gephi_files(imam_data, G, degree_cent, eigenvector_cent, betweenness_cent)
    print("Successfully created gephi_nodes.csv and gephi_edges.csv")
except Exception as e:
    print(f"Error creating Gephi files: {str(e)}")

print(f"\nCurrent working directory: {os.getcwd()}")
print("Script execution completed.")