# Script that remaps the node ids in a graph file starting from 1
import json
import os
import sys

def remap_graph_id(graph_file, community_file=None):
    # Remap the graph
    with open(graph_file, 'r') as f:
        lines = f.readlines()

    header_lines = []
    edge_lines = []

    for line in lines:
        if line.startswith('#'):
            header_lines.append(line)
        elif line.strip():  
            edge_lines.append(line.strip())

    node_ids = set()
    for line in edge_lines:
        u, v = map(int, line.split())
        node_ids.add(u)
        node_ids.add(v)

    # Mapping: Original node -> new sequential id (starting from 1)
    id_mapping = {old_id: new_id for new_id, old_id in enumerate(sorted(node_ids), start=1)}

    # Remap edges in the graph
    remapped_edges = []
    for line in edge_lines:
        u, v = map(int, line.split())
        remapped_edges.append(f"{id_mapping[u]}\t{id_mapping[v]}\n")

    with open(graph_file, 'w') as f:
        f.writelines(header_lines)
        f.write("# FromNodeId\tToNodeId\n")
        f.writelines(remapped_edges)
    print(f"Graph remapped saved in {graph_file}")

    # Remap communities if provided (top5000)
    if community_file:
        with open(community_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]

        remapped_lines = []
        for community_id, line in enumerate(lines, start=1):
            original_ids = map(int, line.split())
            remapped_ids = [str(id_mapping[node_id]) for node_id in original_ids]
            remapped_line = f"{community_id}\t" + "\t".join(remapped_ids)
            remapped_lines.append(remapped_line)

        with open(community_file, 'w') as f:
            f.write("\n".join(remapped_lines))

        print(f"Communities remapped")


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python remap_graph.py <graph_file> [community_file (top5000)]")
        sys.exit(1)

    graph_path = sys.argv[1]
    community_path = sys.argv[2] if len(sys.argv) == 3 else None

    remap_graph_id(graph_path, community_path)
