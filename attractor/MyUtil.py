from pyspark.sql import SparkSession
import os, sys
import shutil
import time
from collections import deque


class MyUtil:

    @staticmethod
    def delete_path(path: str):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)



def breadth_first_search(self, filename, N, outfile):
    E = 0
    n_edge_dis1 = 0
    visited = [0] * N
    comms = [0] * N
    adj_list = [[] for _ in range(N)]

    with open(filename, "r") as reader:
        for line in reader:
            args = line.strip().split()
            u = int(args[0])
            v = int(args[1])

            n = args[2].replace(",", ".")
            if "0." in n:
                dis = 0
            elif "1." in n:
                dis = 1
            else:
                raise AssertionError("Something wrong with implementation")

            assert 1 <= u <= N and 1 <= v <= N
            E += 1

            if dis == 1:
                n_edge_dis1 += 1
                continue

            u -= 1
            v -= 1
            adj_list[u].append(v)
            adj_list[v].append(u)

    # Compute BFS
    queue = deque()
    ID = 0

    for i in range(N):
        if visited[i] == 0:
            # Start new BFS
            queue.clear()
            visited[i] = 1
            queue.append(i)
            ID += 1  # New community
            comms[i] = ID

            while queue:
                curr = queue.popleft()
                assert 0 <= curr < N
                visited[curr] = 1

                for adj in adj_list[curr]:
                    if visited[adj] == 1:
                        continue
                    visited[adj] = 1
                    comms[adj] = ID
                    queue.append(adj)

    with open(outfile, "w") as out:
        for i in range(N):
            out.write(f"{i + 1} {comms[i]}\n")
