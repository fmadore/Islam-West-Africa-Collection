import requests
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from tqdm import tqdm
from pyvis.network import Network
import folium
from folium.plugins import MarkerCluster

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


# List of imam item numbers
imam_ids = [1124, 2150, 1615, 861, 945, 944, 855, 1940, 925]

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

        for doc in tqdm(imam['@reverse'].get('dcterms:subject', []), desc=f"Processing documents for {imam_name}",
                        leave=False):
            try:
                resource_id = doc['@id'].split('/')[-1]
                resource_data = get_resource_data(resource_id)

                imam_data[imam_name]['documents'].append(doc.get('o:title', 'Unknown Document'))
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

# Create edges based on shared documents
for imam1 in imam_data:
    for imam2 in imam_data:
        if imam1 != imam2:
            shared_docs = set(imam_data[imam1]['documents']) & set(imam_data[imam2]['documents'])
            if shared_docs:
                G.add_edge(imam1, imam2, weight=len(shared_docs))

# Network analysis
print("Network Analysis:")
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

degree_cent = nx.degree_centrality(G)
eigenvector_cent = nx.eigenvector_centrality(G, weight='weight')

print("\nTop 3 Imams by Degree Centrality:")
for imam, centrality in sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

print("\nTop 3 Imams by Eigenvector Centrality:")
for imam, centrality in sorted(eigenvector_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
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

# Create an interactive network visualization
net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")

for node in G.nodes():
    net.add_node(node, label=node,
                 title=f"Degree Centrality: {degree_cent[node]:.3f}\nEigenvector Centrality: {eigenvector_cent[node]:.3f}")

for edge in G.edges(data=True):
    net.add_edge(edge[0], edge[1], value=edge[2]['weight'], title=f"Shared Documents: {edge[2]['weight']}")

net.barnes_hut()
net.save_graph("interactive_imam_network.html")

# Create a map visualization
m = folium.Map(location=[12.36566, -1.53388], zoom_start=6)  # Centered on Ouagadougou
marker_cluster = MarkerCluster().add_to(m)

for location in all_locations:
    if location['coordinates']:
        folium.Marker(
            location=[float(location['coordinates'][0]), float(location['coordinates'][1])],
            popup=location['name'],
            tooltip=location['name']
        ).add_to(marker_cluster)

m.save("imam_locations_map.html")

print("\nAnalysis complete. Visualizations saved as 'interactive_imam_network.html' and 'imam_locations_map.html'.")