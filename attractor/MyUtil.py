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

def reduce_edges(
    self,
    N,
    curr_edge_folder,
    hdfs,
    local,
    conf,
    reduced_local_edge_file,
    left_edges_file,
    prev_left_edges_file,
):
    try:
        tic = time.time()

        dirty = [0] * N
        non_converged_edges = 0
        converged_edges = 0

        local_edge_folder = os.path.basename(curr_edge_folder)

        MyUtil.copy_folder(curr_edge_folder, local_edge_folder, hdfs, local, conf)

        list_edges = []
        for filename in os.listdir(local_edge_folder):
            if not filename.startswith("."):
                file_path = os.path.join(local_edge_folder, filename)
                
                edges = MyUtil.read_sequence_file(file_path)
                list_edges.extend(edges)

        MyUtil.fully_delete(local_edge_folder)

        for edge in list_edges:
            u = edge.center
            v = edge.target
            dis = edge.weight

            if 0 < dis < 1:
                u -= 1
                v -= 1
                dirty[u] = 1
                dirty[v] = 1
                non_converged_edges += 1
            else:
                converged_edges += 1

        for edge in list_edges:
            u = edge.center - 1
            v = edge.target - 1

            if dirty[u] == 1 or dirty[v] == 1:
                if dirty[u] == 0:
                    dirty[u] = 2
                if dirty[v] == 0:
                    dirty[v] = 2

        the_number_of_continued_to_used_edges = 0

        with open(reduced_local_edge_file, "w") as writer:
            with open(left_edges_file, "w") as left_edges_writer:
                for edge in list_edges:
                    u = edge.center - 1
                    v = edge.target - 1

                    if dirty[u] in [1, 2] or dirty[v] in [1, 2]:
                        writer.write(f"{edge.center} {edge.target} {edge.weight}\n")
                        the_number_of_continued_to_used_edges += 1
                    else:
                        left_edges_writer.write(
                            edge.to_string_for_local_machine() + "\n"
                        )
                if prev_left_edges_file and os.path.exists(prev_left_edges_file):
                    with open(prev_left_edges_file, "r") as reader1:
                        for line in reader1:
                            left_edges_writer.write(line)

                    os.remove(prev_left_edges_file)

        message = f"#Converged edges: {converged_edges} #Edges of reduced graph: {the_number_of_continued_to_used_edges} #non-converged edges: {non_converged_edges}"
        print(message)
        self.log_job.write(message + "\n")
        self.log_job.flush()

        toc = time.time()
        self.time_updating_edges += toc - tic

        return [
            converged_edges,
            non_converged_edges,
            the_number_of_continued_to_used_edges,
        ]

    except Exception as e:
        print(f"Error in reduce_edges: {e}")
        sys.exit(1)

def validate_final_edges(self, file1, no_original_edges):
    cnt = 0
    with open(file1, "r") as reader:
        for line in reader:
            args = line.strip().split()
            u = int(args[0])
            v = int(args[1])
            dis = float(args[2].replace(",", "."))

            assert abs(dis) < 1e-10 or abs(dis - 1.0) < 1e-10
            cnt += 1

    assert cnt == no_original_edges

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
