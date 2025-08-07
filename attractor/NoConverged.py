from pyspark.sql import SparkSession
import os
import shutil
from collections import deque
from libs.Details import Details

class NoConverged:

    def connected_components(output_second_phase, num_vertices, details: Details):
        visited = [False] * num_vertices
        comms = [0] * num_vertices 
        adj_list = [[] for _ in range(num_vertices)]

        # Build adjacency list, skipping edges with weight == 1
        for edge, (v, weight, sliding, deg_u, deg_v) in output_second_phase:
            if int(weight) == 1:
                continue
            
            u, v = int(edge.split('-')[0]), int(v)
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
        details.n_community = ID

        for i in range(num_vertices):
                details.communities[i+1] = comms[i]

        return comms

    # def save_communities(communities, output_folder, num_vertices):
    #     if not os.path.exists(output_folder):
    #         os.makedirs(output_folder)

    #     with open(f"{output_folder}/communities.txt", "w") as f:
    #         for i in range(num_vertices):
    #             f.write(f"{i + 1} {communities[i]}\n")

    @staticmethod
    def reduce_edges(output_update_edges,):
        non_converged = 0
        
        for row in output_update_edges:
            c, t = row[0].split("-")
            u = int(c)
            v = int(t)
            dis = row[1][1]
        
            if dis < 1 and dis > 0:
                u -= 1
                v -= 1
                non_converged += 1

        
        return non_converged