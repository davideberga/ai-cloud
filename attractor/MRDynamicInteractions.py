from pyspark.sql import Row
from typing import List, Tuple
from attractor.DynamicInteractions import DynamicInteractions


class MRDynamicInteractions:
    @staticmethod
    def mapReduce(
        rdd_star_graph, n_partition: int, _lambda_: float,
    ):
        grouped_by_subgraph = rdd_star_graph.groupByKey()

        computed_dyni = grouped_by_subgraph.flatMap(
            lambda part: MRDynamicInteractions.reduce_function(
                part, n_partition, _lambda_
            )
        )

        return computed_dyni

    @staticmethod
    def reduce_function(
        partition: Tuple[str, List], n_partitions, lambda_
        
        
    ):
        partition_name, star_graphs = partition
        
        # This is slinding data, simply rebroadcast
        if "-" in partition_name:
            return [(partition_name, list(partition[1])[0])]
        
        result = []
        listEdges = []
        sum_degree = 0
        main_edges = 0
        rear_edges = 0
        dictSumWeight = dict()
        adjListDictMain = dict()
        adjListDictForExclusive = dict()
        
        partition_name_splitted = list(map(int, partition_name.split(" ")))

        excluded = dict()
        for star_graph in star_graphs:
            center, deg_center, neighbors = star_graph

            sum_degree += deg_center
            sumWeight = 0.0
            hash_center = DynamicInteractions.node2hash(center, n_partitions)

            for neigh_info in neighbors:
                neighbor_id, neighbor_distance, neighbor_degree = neigh_info

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
                                )
                            )
                        else:
                            excluded[f"{center}-{neighbor_id}"] = (
                                neighbor_id,
                                0,
                                neighbor_distance,
                                deg_center,
                                neighbor_degree,
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
                )
                key = f"{edge.center}-{edge.neighbor}"
                if key in excluded.keys():
                    del excluded[f"{edge.center}-{edge.neighbor}"]
                result.append(attr)

        for key, v in excluded.items():
            print(key)
            result.append((key, v))

        return result
