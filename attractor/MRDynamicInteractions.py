from pyspark.sql import Row
from typing import List, Tuple
from attractor.DynamicInteractions import DynamicInteractions


class MRDynamicInteractions:
    @staticmethod
    def mapReduce(
        rdd_star_graph, n_partition: int, _lambda_: float,
    ):
        intermediate_rdd = rdd_star_graph.flatMap(MRDynamicInteractions.map_function)

        grouped_by_subgraph = intermediate_rdd.groupByKey()

        computed_dyni = grouped_by_subgraph.flatMap(
            lambda part: MRDynamicInteractions.reduce_function(
                part, n_partition, _lambda_
            )
        )

        return computed_dyni

    @staticmethod
    def map_function(star_graph):
        star = star_graph

        center = star.center
        neighbors = star.neighbors
        triplets = star.triplets
        degree = star.degree

        results = [
            (triplet, Row(center=center, degree=degree, neighbors=neighbors, triplets=triplets))
            for triplet in triplets
        ]
        return results

    @staticmethod
    def reduce_function(
        partition: Tuple[str, List], n_partitions, lambda_
    ):
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

        excluded = dict()
        for star_graph in star_graphs:
            center = star_graph.center
            deg_center = star_graph.degree
            neighbors = star_graph.neighbors
            triplets = star_graph.triplets

            sum_degree += deg_center
            sumWeight = 0.0
            hash_center = DynamicInteractions.node2hash(center, n_partitions)

            for neigh_info in neighbors:
                neighbor_id = neigh_info.vertex_id  # prima era v
                neighbor_distance = neigh_info.weight
                neighbor_degree = neigh_info.degree
                neighbor_edge_sliding = neigh_info.sliding

                sumWeight += 1.0 - neighbor_distance

                adj = neigh_info

                hash_neighbor = DynamicInteractions.node2hash(neighbor_id, n_partitions)

                if (
                    hash_center in partition_name_splitted
                    and hash_neighbor in partition_name_splitted
                ):
                    if center > neighbor_id:
                        if neighbor_distance < 1 and neighbor_distance > 0:
                            listEdges.append(
                                Row(
                                    center=center,
                                    neighbor=neighbor_id,
                                    weight=neighbor_distance,
                                    deg_center=deg_center,
                                    deg_neigh= neighbor_degree,
                                    triplets=triplets,
                                    sliding=neighbor_edge_sliding
                                )
                            )
                        else:
                            excluded[f"{center}-{neighbor_id}"] = (
                                "I",
                                neighbor_id,
                                0,
                                neigh_info.weight,
                                deg_center,
                                neighbor_degree,
                                neighbor_edge_sliding
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
            if edge.weight < 1 and edge.weight > 0:
                deg_u = edge.deg_center
                deg_v = edge.deg_neigh
                attr = DynamicInteractions.union_intersection(
                    edge.center,
                    edge.neighbor,
                    deg_u,
                    deg_v,
                    adjListDictMain,
                    adjListDictForExclusive,
                    dictSumWeight,
                    n_partitions,
                    edge.weight,
                    partition_name_splitted,
                    lambda_,
                    edge.triplets,
                    edge.sliding
                )
                key = f"{edge.center}-{edge.neighbor}"
                if key in excluded.keys():
                    del excluded[f"{edge.center}-{edge.neighbor}"]
                result.append(attr)

        for key, v in excluded.items():
            result.append((key, v))

        return result
