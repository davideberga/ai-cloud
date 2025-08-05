# Script that remaps the node ids in a graph file starting from 1

def remap_graph_id(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    header_lines = []
    edge_lines = []

    for line in lines:
        if line.startswith('#'):
            header_lines.append(line)
        elif line.strip():  # Evita righe vuote
            edge_lines.append(line.strip())
 
    node_ids = set()
    for line in edge_lines:
        u, v = map(int, line.split())
        node_ids.add(u)
        node_ids.add(v)

    # Mapping: Original node -> new sequential id (starting from 1)
    id_mapping = {old_id: new_id for new_id, old_id in enumerate(sorted(node_ids), start=1)}

    # Add new remmapped edges
    remapped_edges = []
    for line in edge_lines:
        u, v = map(int, line.split())
        new_u = id_mapping[u]
        new_v = id_mapping[v]
        remapped_edges.append(f"{new_u}\t{new_v}\n")

    # Rewriting the file with remapped edges
    with open(file_path, 'w') as f:
        f.writelines(header_lines)
        f.write("# FromNodeId\tToNodeId\n")
        f.writelines(remapped_edges)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Use: python remap_graph.py <file_path>")
    else:
        remap_graph_id(sys.argv[1])
