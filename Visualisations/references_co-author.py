import requests
from collections import defaultdict
from tqdm import tqdm
import plotly.graph_objs as go
import networkx as nx

# API URL and item set identifiers
api_url = "https://iwac.frederickmadore.com/api"
country_item_sets = {
    2193: 'Bénin',
    2212: 'Burkina Faso',
    2217: 'Côte d\'Ivoire',
    2222: 'Niger',
    2225: 'Nigeria',
    2228: 'Togo'
}

def fetch_items(item_set_id):
    """ Fetch items from the API based on the item set ID. """
    items = []
    page = 1
    total_pages = 1  # Initially assumed to be 1
    pbar = tqdm(total=total_pages, desc=f"Fetching items for ID {item_set_id}")
    while page <= total_pages:
        response = requests.get(f"{api_url}/items", params={"item_set_id": item_set_id, "page": page, "per_page": 50})
        data = response.json()
        if not data:
            break
        if page == 1:
            total_result_header = response.headers.get('Omeka-S-Total-Results', str(len(data)))
            if total_result_header.isdigit():
                total_pages = int(total_result_header) // 50 + 1
            pbar.total = total_pages
        items.extend(data)
        page += 1
        pbar.update()
    pbar.close()
    return items


def parse_authors(items):
    """ Parse the authors from items and create a map of co-authorships. """
    author_details = defaultdict(str)
    co_author_map = defaultdict(set)

    for item in items:
        authors = item.get('bibo:authorList', [])
        author_ids = set()

        for author in authors:
            author_id = author.get('value_resource_id')
            display_title = author.get('display_title', 'Unknown')
            if author_id:
                author_ids.add(author_id)
                author_details[author_id] = display_title

        # Update the co-author map
        for author_id in author_ids:
            co_author_map[author_id].update(author_ids - {author_id})

    return author_details, co_author_map


def visualize_interactive_co_author_network(co_authors, author_details):
    # Create a new graph
    G = nx.Graph()

    # Add nodes and edges to the graph
    for author_id, co_author_ids in co_authors.items():
        for co_author_id in co_author_ids:
            # Ensure both authors are in the author_details
            if author_id in author_details and co_author_id in author_details:
                G.add_edge(author_details[author_id], author_details[co_author_id])

    # Generate positions for each node
    pos = nx.spring_layout(G)

    # Create traces for edges
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    # Create traces for nodes
    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(f"{node} (# of connections: {len(G[node])})")

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            reversescale=True,
            size=[len(G[node]) * 10 for node in G.nodes()],  # Adjust the size multiplier as needed
            color=[],
            colorbar=dict(
                thickness=15,
                title='Number of Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))

    # Set color for the nodes
    node_adjacencies = []
    for node, adjacencies in enumerate(G.adjacency()):
        node_adjacencies.append(len(adjacencies[1]))
    node_trace.marker.color = node_adjacencies

    # Create the figure
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Co-authorship network',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        annotations=[dict(
                            text="",
                            showarrow=False,
                            xref="paper", yref="paper",
                            x=0.005, y=-0.002 )],
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

    fig.show()
    fig.write_html('file.html')
    return fig  # Return the figure for further use if needed


def main():
    for item_set_id, country in country_item_sets.items():
        items = fetch_items(item_set_id)
        author_details, co_authors = parse_authors(items)
        fig = visualize_interactive_co_author_network(co_authors, author_details)
        # Save the figure as an HTML file
        fig.write_html(f'co_authorship_network_{country}.html')

if __name__ == "__main__":
    main()
