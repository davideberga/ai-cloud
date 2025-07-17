from pyspark.sql import Row
from pyspark.sql import SparkSession
import logging
from typing import List, Dict, Tuple
from collections import defaultdict
from args_parser import parse_arguments
from attractor.DynamicInteractions import DynamicInteractions


class MRDynamicInteractions:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.logger = logging.getLogger("LoopDynamicInteractions")

    def mapReduce(
        self, rdd_star_graph, n_partition: int, _lambda_: float, df_degree_broadcasted
    ):
        partitions = n_partition
        lambda_ = _lambda_

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
            result = []

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

                p_u = DynamicInteractions.node2hash(u, partitions)  # hash of u
                p_v = DynamicInteractions.node2hash(v, partitions)  # hash of v
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
                        p_neighbor = DynamicInteractions.node2hash(neighbor, partitions)
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

                print("SONO IN REDUCE FUNCTION")
                for chiave, valore in dictSumWeight.items():
                    print(f"{chiave}: {valore}")

                for edge in listEdges:
                    u = edge.center
                    v = edge.neighbor
                    distance = edge.distance

                    if 0 < distance < 1:
                        deg_u = df_degree_broadcasted.value.get(u)
                        deg_v = df_degree_broadcasted.value.get(v)

                        attr = DynamicInteractions.union_intersection(
                            u,
                            v,
                            deg_u,
                            deg_v,
                            adjListDictMain,
                            adjListDictForExclusive,
                            dictSumWeight,
                            partitions,
                            distance,
                            partition_name_splitted,
                            lambda_,
                        )
                        result.append(attr)

            return result
        
        print("Compute Dynamic Interactions")
        intermediate_rdd = rdd_star_graph.flatMap(map_function)
        grouped_by_subgraph = intermediate_rdd.groupByKey()
        computed_dyni = grouped_by_subgraph.map(reduce_function)
        print("Compute Dynamic Interactions END")
        return computed_dyni
