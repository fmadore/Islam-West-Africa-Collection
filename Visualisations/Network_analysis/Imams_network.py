import requests
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter
from tqdm import tqdm
from pyvis.network import Network

# List of 10 imam item numbers
imam_ids = [1124, 2150, 1615, 861, 945, 944, 855, 1940, 925]

base_url = "https://iwac.frederickmadore.com/api"


def get_imam_data(imam_id):
    response = requests.get(f"{base_url}/items/{imam_id}")
    return response.json()


def get_resource_data(resource_id):
    response = requests.get(f"{base_url}/resources/{resource_id}")
    return response.json()


def extract_subjects(resource_data):
    return [subject['display_title'] for subject in resource_data.get('dcterms:subject', [])]


def extract_locations(resource_data):
    return [location['display_title'] for location in resource_data.get('dcterms:spatial', [])]


# Create a graph
G = nx.Graph()

# Collect data for each imam
imam_data = {}
for imam_id in tqdm(imam_ids, desc="Processing imams"):
    imam = get_imam_data(imam_id)
    imam_name = imam['o:title']
    G.add_node(imam_name)
    imam_data[imam_name] = {
        'documents': [],
        'subjects': [],
        'locations': []
    }

    # Process each document mentioning the imam
    for doc in tqdm(imam['@reverse']['dcterms:subject'], desc=f"Processing documents for {imam_name}", leave=False):
        resource_id = doc['@id'].split('/')[-1]
        resource_data = get_resource_data(resource_id)

        imam_data[imam_name]['documents'].append(doc['o:title'])
        imam_data[imam_name]['subjects'].extend(extract_subjects(resource_data))
        imam_data[imam_name]['locations'].extend(extract_locations(resource_data))

# Create edges based on shared documents
for imam1 in imam_data:
    for imam2 in imam_data:
        if imam1 != imam2:
            shared_docs = set(imam_data[imam1]['documents']) & set(imam_data[imam2]['documents'])
            if shared_docs:
                G.add_edge(imam1, imam2, weight=len(shared_docs))

# Analyze the network
print("Network Analysis:")
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

# Centrality measures
degree_cent = nx.degree_centrality(G)
eigenvector_cent = nx.eigenvector_centrality(G, weight='weight')

print("\nTop 3 Imams by Degree Centrality:")
for imam, centrality in sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

print("\nTop 3 Imams by Eigenvector Centrality:")
for imam, centrality in sorted(eigenvector_cent.items(), key=lambda x: x[1], reverse=True)[:3]:
    print(f"{imam}: {centrality:.3f}")

# Analyze common subjects and locations
all_subjects = [subject for imam in imam_data.values() for subject in imam['subjects']]
all_locations = [location for imam in imam_data.values() for location in imam['locations']]

print("\nTop 5 Common Subjects:")
for subject, count in Counter(all_subjects).most_common(5):
    print(f"{subject}: {count}")

print("\nTop 5 Common Locations:")
for location, count in Counter(all_locations).most_common(5):
    print(f"{location}: {count}")

# Create an interactive network visualization
net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")

# Add nodes
for node in G.nodes():
    net.add_node(node, label=node,
                 title=f"Degree Centrality: {degree_cent[node]:.3f}\nEigenvector Centrality: {eigenvector_cent[node]:.3f}")

# Add edges
for edge in G.edges(data=True):
    net.add_edge(edge[0], edge[1], value=edge[2]['weight'], title=f"Shared Documents: {edge[2]['weight']}")

# Set physics layout
net.barnes_hut()

# Save the interactive visualization
net.save_graph("interactive_imam_network.html")

print("\nAnalysis complete. Interactive network visualization saved as 'interactive_imam_network.html'.")

# You can still keep the matplotlib visualization if needed
plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G)
nx.draw_networkx_nodes(G, pos, node_size=1000, node_color='lightblue')
nx.draw_networkx_labels(G, pos, font_size=8)
edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
nx.draw_networkx_edges(G, pos, width=edge_weights, alpha=0.7)
plt.title("Network of Key Imams")
plt.axis('off')
plt.tight_layout()
plt.savefig("imam_network.png", dpi=300, bbox_inches='tight')