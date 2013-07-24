import json
import networkx as nx
from networkx.readwrite import json_graph
import requests

G = nx.DiGraph()
topictree = requests.get("http://www.khanacademy.org/api/v1/topictree").json()
def get_nodes(top):
    if 'node_slug' in top and 'e/' in top['node_slug']:
        G.add_node(top['node_slug'][2:])
    if 'children' in top:
        for child in top['children']:
            get_nodes(child)

def get_edges(top):
    if 'covers' in top:
        for covered in top['covers']:
            G.add_edge(top['node_slug'][2:], covered)
    if 'children' not in top or not top['children']:
        return
    for child in top['children']:
        get_edges(child)

get_nodes(topictree)
get_edges(topictree)
d = json_graph.node_link_data(G)
with open('../webapps/dashboards/static/cover_graph.json', 'wb') as f:
    json.dump(d, f)