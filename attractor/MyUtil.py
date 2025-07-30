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
    n_edge_dis1 = 0
    visited = [0] * num_vertices
    comms = [0] * num_vertices
    adj_list = [[] for _ in range(num_vertices)]

    for row in output_second_phase:
        u, (t, v, weight, sliding) = row
        dis = 0
        if "0." in str(weight):
            dis = 0
        elif "1." in str(weight):
            dis = 1
        else:
            raise AssertionError("Something wrong in Breath First Search")

        if dis == 1: # Edge with distance 1 wiil be discarded
            n_edge_dis1 += 1
            continue
        # Note to reduce this vertex
        u -= 1
        v -= 1
        adj_list[u].append(v)
        adj_list[v].append(u)

    # Compute BFS
    queue = deque()
    ID = 0

    for i in range(num_vertices):
        if visited[i] == 0:
            # Start new BFS
            queue.clear()
            visited[i] = 1
            queue.append(i)
            ID += 1  # New community
            comms[i] = ID
            # These vertices are part of the same community
            while queue:
                curr = queue.popleft()
                assert 0 <= curr < num_vertices
                visited[curr] = 1

                for adj in adj_list[curr]:
                    if visited[adj] == 1:
                        continue
                    visited[adj] = 1
                    comms[adj] = ID
                    queue.append(adj)

    # Print type of community (comms) for each vertex
    # for i in range(num_vertices):
    #     print(f"{i + 1} {comms[i]}")

    n_comms = max(comms)
    print(f"Number of communities: {n_comms}")

    return comms

def save_communities(communities, output_folder, num_vertices):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(f"{output_folder}/communities.txt", "w") as f:
        for i in range(num_vertices):
            f.write(f"{i + 1} {communities[i]}\n")