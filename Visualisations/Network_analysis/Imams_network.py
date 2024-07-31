import requests
import networkx as nx
from collections import Counter, defaultdict
from tqdm import tqdm
import csv
from datetime import datetime
from itertools import combinations

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


def export_palladio_nodes(imam_data, degree_cent, eigenvector_cent, betweenness_cent, filename='palladio_nodes.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Id', 'Label', 'Type', 'Degree Centrality', 'Eigenvector Centrality', 'Betweenness Centrality',
                      'Document Count', 'Unique Subjects', 'Unique Locations']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for imam, data in imam_data.items():
            writer.writerow({
                'Id': imam,
                'Label': imam,
                'Type': 'Imam',
                'Degree Centrality': degree_cent[imam],
                'Eigenvector Centrality': eigenvector_cent[imam],
                'Betweenness Centrality': betweenness_cent[imam],
                'Document Count': len(data['documents']),
                'Unique Subjects': sum(len(set(subjects)) for subjects in data['subjects'].values()),
                'Unique Locations': len(set(loc['name'] for loc in data['locations']))
            })


def export_palladio_edges(G, filename='palladio_edges.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Source', 'Target', 'Weight', 'Shared Subjects', 'Shared Locations']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for edge in G.edges(data=True):
            writer.writerow({
                'Source': edge[0],
                'Target': edge[1],
                'Weight': edge[2]['weight'],
                'Shared Subjects': edge[2]['shared_subjects'],
                'Shared Locations': edge[2]['shared_locations']
            })


def export_palladio_subjects(imam_data, filename='palladio_subjects.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Imam', 'Subject', 'Category', 'Frequency']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for imam, data in imam_data.items():
            for category, subjects in data['subjects'].items():
                subject_counts = Counter(subjects)
                for subject, count in subject_counts.items():
                    writer.writerow({
                        'Imam': imam,
                        'Subject': subject,
                        'Category': category,
                        'Frequency': count
                    })


def export_palladio_locations(imam_data, filename='palladio_locations.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Imam', 'Location', 'Latitude', 'Longitude', 'Frequency']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for imam, data in imam_data.items():
            location_counts = Counter(loc['name'] for loc in data['locations'])
            for location in data['locations']:
                if location['coordinates']:
                    writer.writerow({
                        'Imam': imam,
                        'Location': location['name'],
                        'Latitude': location['coordinates'][0],
                        'Longitude': location['coordinates'][1],
                        'Frequency': location_counts[location['name']]
                    })


def export_palladio_timeline(imam_data, filename='palladio_timeline.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Imam', 'Date', 'Document Count', 'Document Titles']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for imam, data in imam_data.items():
            date_docs = defaultdict(list)
            for doc in data['documents']:
                if doc['date']:
                    date_docs[doc['date']].append(doc['title'])

            for date, docs in date_docs.items():
                writer.writerow({
                    'Imam': imam,
                    'Date': date,
                    'Document Count': len(docs),
                    'Document Titles': '; '.join(docs)
                })


# List of imam item numbers
imam_ids = [1124, 2150]

# Create a graph
G = nx.Graph()

# Collect data for each imam
imam_data = {}
all_locations = []
all_subjects = defaultdict(list)

for imam_id in tqdm(imam_ids, desc="Processing imams"):
    try:
        imam = get_imam_data(imam_id)
        imam_name = imam['o:title']
        G.add_node(imam_name)
        imam_data[imam_name] = {
            'documents': [],
            'subjects': defaultdict(list),
            'locations': []
        }

        for doc in tqdm(imam['@reverse'].get('dcterms:subject', []), desc=f"Processing documents for {imam_name}", leave=False):
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
    except Exception as e:
        print(f"Error processing imam with ID {imam_id}: {str(e)}")

# Create edges based on shared documents, subjects, and locations
for imam1, imam2 in combinations(imam_data.keys(), 2):
    shared_docs = set(doc['title'] for doc in imam_data[imam1]['documents']) & set(
        doc['title'] for doc in imam_data[imam2]['documents'])
    shared_subjects = set.union(*[set(imam_data[imam1]['subjects'][cat]) for cat in imam_data[imam1]['subjects']]) & \
                      set.union(*[set(imam_data[imam2]['subjects'][cat]) for cat in imam_data[imam2]['subjects']])
    shared_locations = set(loc['name'] for loc in imam_data[imam1]['locations']) & set(
        loc['name'] for loc in imam_data[imam2]['locations'])

    if shared_docs or shared_subjects or shared_locations:
        G.add_edge(imam1, imam2,
                   weight=len(shared_docs),
                   shared_subjects=len(shared_subjects),
                   shared_locations=len(shared_locations))

# Network analysis
print("Network Analysis:")
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

degree_cent = nx.degree_centrality(G)
eigenvector_cent = nx.eigenvector_centrality(G, weight='weight')
betweenness_cent = nx.betweenness_centrality(G, weight='weight')

print("\nTop 3 Imams by Degree Centrality:")
for imam, centrality in sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

print("\nTop 3 Imams by Eigenvector Centrality:")
for imam, centrality in sorted(eigenvector_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

print("\nTop 3 Imams by Betweenness Centrality:")
for imam, centrality in sorted(betweenness_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

# Analyze common subjects by category
print("\nTop 5 Common Subjects by Category:")
for category in ['Topic', 'Event', 'Person', 'Organization']:
    print(f"\n{category}:")
    for subject, count in Counter(all_subjects[category]).most_common(5):
        print(f"{subject}: {count}")

print("\nTop 5 Common Locations:")
location_counter = Counter(loc['name'] for loc in all_locations)
for location, count in location_counter.most_common(5):
    print(f"{location}: {count}")

# Export data for Palladio
export_palladio_nodes(imam_data, degree_cent, eigenvector_cent, betweenness_cent)
export_palladio_edges(G)
export_palladio_subjects(imam_data)
export_palladio_locations(imam_data)
export_palladio_timeline(imam_data)

print("\nAnalysis complete. Data exports created for Palladio:")
print("1. palladio_nodes.csv - Imam data")
print("2. palladio_edges.csv - Network connections")
print("3. palladio_subjects.csv - Subject data")
print("4. palladio_locations.csv - Location data")
print("5. palladio_timeline.csv - Timeline data")