from pyspark.sql import SparkSession
import os
import shutil
from collections import deque

class MyUtil:

    @staticmethod
    def delete_path(path: str):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)

def breadth_first_search(output_second_phase, num_vertices):
    visited = [False] * num_vertices
    comms = [0] * num_vertices
    adj_list = [[] for _ in range(num_vertices)]

    # Build adjacency list, skipping edges with weight == 1
    for _, u, v, weight in output_second_phase:
        if int(weight) == 1:
            continue
        u -= 1
        v -= 1
        adj_list[u].append(v)
        adj_list[v].append(u)

    # BFS for connected components
    ID = 0
    for i in range(num_vertices):
        if not visited[i]:
            ID += 1
            queue = deque([i])
            visited[i] = True
            comms[i] = ID

            while queue:
                curr = queue.popleft()
                for adj in adj_list[curr]:
                    if not visited[adj]:
                        visited[adj] = True
                        comms[adj] = ID
                        queue.append(adj)

    print(f"Number of communities: {ID}")
    return comms

def save_communities(communities, output_folder, num_vertices):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(f"{output_folder}/communities.txt", "w") as f:
        for i in range(num_vertices):
            f.write(f"{i + 1} {communities[i]}\n")