import json
from roger.core import Util
from roger.Config import get_default_config
import argparse

def create_node(node_id):
    return {
        "id": node_id,
        "category": ["biolink:NamedThing"],
        "label": node_id
    }

def create_edge(source_id, target_id):
    predicate = "biolink:related_to"
    return {
        "id" : f"{source_id}-{predicate}-{target_id}",
        "subject": source_id,
        "object": target_id,
        "predicate": predicate,
        "type": predicate
    }


def generate_graph(nodes_size, id_salt):
    """
    Generates a graph with `nodes_size` nodes and `nodes_size`/2 edges
    """
    graph = {
        "nodes": [],
        "edges": []
    }
    for x in range(0 ,nodes_size):
        graph["nodes"].append( create_node(f'{id_salt}:{x}'))
    nodes_size = nodes_size - nodes_size %2 # normalizes odd numbers graph sizes
    for x in range(0, nodes_size, 2):
        graph["edges"].append(create_edge(source_id=f'{id_salt}:{x}', target_id=f'{id_salt}:{x + 1}'))
    return graph


def generate_kgx_files(file_count, node_size, directory):
    id_salt_generator = lambda x: f'GRAPH{x}'
    for i in range(0, file_count):
        import os
        Util.mkdir(directory)
        file = os.path.join(directory, f'graph_{i}.json')
        with open(file, 'w') as f:
            graph = generate_graph(node_size, id_salt_generator(i))
            json.dump(graph, f)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Roger common cli tool.')
    """ Common CLI. """
    config = get_default_config()
    parser.add_argument('-f', '--files-count', help="Number of kgx files", default=100)
    parser.add_argument('-n', '--nodes-count', help="Number of nodes in each file, note each graph would contain (n - n%2)/2 edges (half of nodes) ", default=100)
    parser.add_argument('-d', '--dir', help="Directory to write kgx files to", default=Util.kgx_path(''))

    args = parser.parse_args()

    if args.files_count:
        files_count = int(args.files_count)
    if args.nodes_count:
        nodes_count = int(args.nodes_count)
    if args.dir:
        data_dir = str(args.dir)
    print(f'generating {files_count} kgx files each with {nodes_count} nodes and {int((nodes_count - nodes_count %2)/2)} edges in directory {data_dir}')
    generate_kgx_files(file_count=files_count, node_size=nodes_count, directory=data_dir)