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
            star = star_graph

            center = star.center
            neighbors = star.neighbors
            triplets = star.triplets
            degree = df_degree_broadcasted.value.get(center, 0)

            results = [
                (triplet, Row(center=center, degree=degree, neighbors=neighbors))
                for triplet in triplets
            ]
            return results

        # def reduce_function(partition: Tuple[str, List]):
        #     stars = dict()
        #     s_m = []
        #     result = []

        #     partition_name, star_graphs = partition
        #     partition_name_splitted = list(map(int, partition_name.split(" ")))

        #     for star_graph in star_graphs:
        #         u = star_graph.center
        #         neighbors = star_graph.neighbors
        #         p_u = DynamicInteractions.node2hash(u, partitions)

        #         stars[u] = neighbors

        #         for n in neighbors:
        #             v = n.vertex_id
        #             d = n.weight

        #             p_v = DynamicInteractions.node2hash(v, partitions)

        #             if (
        #                 p_u in partition_name_splitted
        #                 and p_v in partition_name_splitted
        #             ):
        #                 # (u, v) is a main edge
        #                 first = u if u > v else v
        #                 second = u if u < v else v
        #                 s_m.append((first, second, d))

        #     for u, v, d in s_m:
        #         if d == 1 or d == 0:
        #             continue

        #         p_u = DynamicInteractions.node2hash(u, partitions)  # hash of u
        #         p_v = DynamicInteractions.node2hash(v, partitions)  # hash of v

        #         adjListDictForExclusive = dict()
        #         adjListDictMain = dict()
        #         listEdges = []
        #         dictSumWeight = dict()
        #         main_edges, rear_edges, sum_degree = 0, 0, 0

        #         for star_graph in star_graphs:
        #             center = star_graph.center
        #             degree_center = star_graph.degree

        #             sum_degree += degree_center
        #             sum_weight = 0

        #             for n in neighbors:
        #                 neighbor = n.vertex_id
        #                 d = n.weight
        #                 p_neighbor = DynamicInteractions.node2hash(neighbor, partitions)
        #                 sum_weight += 1 - d
                        
        #                 if p_neighbor in partition_name_splitted:
        #                     if center > neighbor:
        #                         if 0 < d < 1:
        #                             listEdges.append(
        #                                 Row(
        #                                     center=center, neighbor=neighbor, distance=d
        #                                 )
        #                             )
        #                         main_edges += 1

        #                     if center not in adjListDictMain.keys():
        #                         adjListDictMain[center] = []
        #                     adjListDictMain[center].append(n)
        #                 else:
        #                     rear_edges += 1

        #                 if center not in adjListDictForExclusive.keys():
        #                     adjListDictForExclusive[center] = []
        #                 adjListDictForExclusive[center].append(n)

        #             dictSumWeight[center] = sum_weight

        #         for edge in listEdges:
        #             c = edge.center
        #             n = edge.neighbor
        #             distance = edge.distance

        #             if 0 < distance < 1:
        #                 deg_c = df_degree_broadcasted.value.get(c)
        #                 deg_n = df_degree_broadcasted.value.get(n)

        #                 attr = DynamicInteractions.union_intersection(
        #                     c,
        #                     n,
        #                     deg_c,
        #                     deg_n,
        #                     adjListDictMain,
        #                     adjListDictForExclusive,
        #                     dictSumWeight,
        #                     partitions,
        #                     distance,
        #                     partition_name_splitted,
        #                     lambda_,
        #                 )
        #                 result.append(attr)

        #     return result

        def reduce_function(partition: Tuple[str, List]):
            stars = dict()
            s_m = []
            result = []
            listEdges = []
            sum_degree = 0
            main_edges = 0
            rear_edges = 0
            dictSumWeight = dict()
            adjListDictMain = dict()
            adjListDictForExclusive = dict()
            partition_name, star_graphs = partition
            partition_name_splitted = list(map(int, partition_name.split(" ")))

            for star_graph in star_graphs:
                center = star_graph.center # prima era u
                deg_center = star_graph.degree 
                neighbors = star_graph.neighbors

                sum_degree += deg_center
                sumWeight = 0.0
                hash_center = DynamicInteractions.node2hash(center, partitions)

                for neigh_info in neighbors:
                    neighbor_id = neigh_info.vertex_id # prima era v
                    neighbor_distance = neigh_info.weight

                    sumWeight += (1.0 - neighbor_distance)

                    adj = neigh_info

                    hash_neighbor = DynamicInteractions.node2hash(neighbor_id, partitions)

                    if (
                        hash_center in partition_name_splitted
                        and hash_neighbor in partition_name_splitted
                    ):
                        if center > neighbor_id:
                            if neighbor_distance < 1 and neighbor_distance > 0:
                                listEdges.append(
                                    Row(
                                        center=center, neighbor=neighbor_id, distance=neighbor_distance
                                    )
                                )
                            main_edges += 1

                        if center in adjListDictMain:
                            adjListDictMain[center].append(adj)
                        else:
                            adjListDictMain[center] = [adj]
                    else:
                        rear_edges += 1

                    if center in adjListDictForExclusive:
                        adjListDictForExclusive[center].append(adj)
                    else:
                        adjListDictForExclusive[center] = [adj]
                
                dictSumWeight[center] = sumWeight

            for edge in listEdges:
                if edge.distance < 1 and edge.distance > 0:
                    deg_u = df_degree_broadcasted.value.get(edge.center, 0)
                    deg_v = df_degree_broadcasted.value.get(edge.neighbor, 0)
                    attr = DynamicInteractions.union_intersection(
                        edge.center,
                        edge.neighbor,
                        deg_u,
                        deg_v,
                        adjListDictMain,
                        adjListDictForExclusive,
                        dictSumWeight,
                        partitions,
                        edge.distance,
                        partition_name_splitted,
                        lambda_,
                    )
                    result.append(attr)

            return result
        
        print("Compute Dynamic Interactions")
        intermediate_rdd = rdd_star_graph.flatMap(map_function)
        grouped_by_subgraph = intermediate_rdd.groupByKey()
        computed_dyni = grouped_by_subgraph.flatMap(reduce_function)

        print("Compute Dynamic Interactions END")
        return computed_dyni
