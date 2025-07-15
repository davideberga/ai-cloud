from pyspark.sql import Row
from pyspark.sql import SparkSession
import logging
from typing import List, Dict, Tuple
from collections import defaultdict

from attractor.DynamicInteractions import DynamicInteractions


class MRDynamicInteractions:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.logger = logging.getLogger("LoopDynamicInteractions")

    def mapReduce(
        self, rdd_star_graph, n_partition: int, lambda_: float, df_degree_broadcasted
    ):
        partitions = n_partition
        lambda_value = lambda_

        def node_to_hash(u: int, no_partitions: int):
            return u % no_partitions

        def map_function(star_graph):
            star = star_graph[0]

            center = star.center
            neighbors = star.neighbors
            triplets = star.triplets
            degree = df_degree_broadcasted.value.get(center, 0)

            results = [
                (triplet, Row(center=center, degree=degree, neighbors=neighbors))
                for triplet in triplets
            ]

            return results

        def reduce_function(partition: Tuple[str, List]):
            stars = dict()
            s_m = []

            partition_name, star_graphs = partition
            partition_name_splitted = list(map(int, partition_name.split(" ")))

            for star_graph in star_graphs:
                u = star_graph.center
                neighbors = star_graph.neighbors
                p_u = DynamicInteractions.node2hash(u, partitions)

                stars[u] = neighbors

                for n in neighbors:
                    v = n.vertex_id
                    d = n.weight

                    p_v = DynamicInteractions.node2hash(v, partitions)

                    if (
                        p_u in partition_name_splitted
                        and p_v in partition_name_splitted
                    ):
                        # (u, v) is a main edge
                        first = u if u > v else v
                        second = u if u < v else v
                        s_m.append((first, second, d))

            for u, v, d in s_m:
                if d == 1 or d == 0:
                    continue

                p_u = DynamicInteractions.node2hash(u)
                p_v = DynamicInteractions.node2hash(v)
                deg_u = df_degree_broadcasted.value.get(u, 0)
                deg_v = df_degree_broadcasted.value.get(v, 0)

                sum_interactions = 0
                sum_interactions += DynamicInteractions.compute_di(
                    p_u, p_v, partitions, d, deg_u, deg_v
                )

                adjListDictForExclusive = dict()
                adjListDictMain = dict()
                listEdges = []
                dictSumWeight = dict()
                main_edges, rear_edges, sum_degree = 0, 0, 0

                for star_graph in star_graphs:
                    center = star_graph.center
                    degree_center = star_graph.degree

                    sum_degree += degree_center
                    sum_weight = 0

                    for n in neighbors:
                        neighbor = n.vertex_id
                        d = n.weight
                        p_neighbor = DynamicInteractions.node2hash(neighbor)
                        sum_weight += 1 - d

                        if p_neighbor in partition_name_splitted:
                            if center > neighbor:
                                if 0 < d < 1:
                                    listEdges.append(
                                        Row(
                                            center=center, neighbor=neighbor, distance=d
                                        )
                                    )
                                main_edges += 1

                            if center not in adjListDictMain.keys():
                                adjListDictMain[center] = []
                            adjListDictMain[center].append(n)
                        else:
                            rear_edges += 1
                        
                        if center not in adjListDictForExclusive.keys():
                            adjListDictForExclusive[center] = []
                        adjListDictForExclusive[center].append(n)

                    dictSumWeight[center] = sum_weight
                
                for edge in listEdges:
                    u = edge.center
                    v = edge.neighbor
                    distance = edge.distance
                    
                    if 0 < distance < 1:
                        deg_u = df_degree_broadcasted.value.get(u)
                        deg_v = df_degree_broadcasted.value.get(v)
                        
                        # DynamicInteractions.union_intersection(
                            
                        # )
                
                        

        intermediate_rdd = rdd_star_graph.flatMap(map_function)
        grouped_by_subgraph = intermediate_rdd.groupByKey()

        # print(grouped_by_subgraph.take(1)[0][0])
        # print(list(grouped_by_subgraph.take(1)[0][1]))
        return grouped_by_subgraph

        # Step 4: GroupByKey to simulate shuffle by TripleWritable key
        grouped_rdd = intermediate_rdd.groupByKey()

        # Step 5: Reduce (simula la reduce di Hadoop)
        result_rdd = grouped_rdd.flatMap(
            lambda kv: self.reduce_function(kv[0], kv[1], lambda_val, bcast_deg.value)
        )

        return result_rdd

    def reduce_function(
        self,
        triplet_key: Tuple[int, int, int],
        stars,
        lambda_val: float,
        map_deg: Dict[int, int],
    ):
        components = set(triplet_key)
        adj_main = defaultdict(list)
        adj_full = defaultdict(list)
        sum_weights = {}
        edges = []

        # Parse stars and build adjacency
        for star in stars:
            center = star["center"]
            deg_center = star["deg"]
            neighbors = star["neighbors"]

            sum_w = 0
            for n in neighbors:
                neighbor = n["lab"]
                dis = n["dis"]
                sum_w += 1 - dis

                adj_full[center].append(n)

                if self.is_main_node(neighbor, components):
                    adj_main[center].append(n)
                    if center > neighbor and 0 < dis < 1:
                        edges.append((center, neighbor, dis))

            sum_weights[center] = sum_w

        # Reduce logic: compute dynamic interactions per edge
        output = []
        for u, v, dis in edges:
            deg_u = map_deg.get(u, 1)
            deg_v = map_deg.get(v, 1)
            result = self._union_intersection(
                u,
                v,
                deg_u,
                deg_v,
                adj_main,
                adj_full,
                sum_weights,
                triplet_key,
                lambda_val,
            )
            output.extend(result)

        return output

    def is_main_node(self, node_id: int, components: set):
        return (node_id % len(components)) in components
