from pyspark.sql import Row
from typing import List, Tuple
from attractor.DynamicInteractions import DynamicInteractions


class MRDynamicInteractions:
    @staticmethod
    def mapReduce(rdd_star_graph, n_partition: int, _lambda_: float, df_degree_broadcasted):
         
        print(rdd_star_graph.take(5))
        exit(0)
        intermediate_rdd = rdd_star_graph.flatMap(
            lambda sg: MRDynamicInteractions.map_function(sg, df_degree_broadcasted.value)
        )

        grouped_by_subgraph = intermediate_rdd.groupByKey()

        computed_dyni = grouped_by_subgraph.flatMap(
            lambda part: MRDynamicInteractions.reduce_function(
                part, df_degree_broadcasted.value, n_partition, _lambda_
            )
        )
        return computed_dyni
    
    @staticmethod
    def map_function(star_graph, df_degree_broadcasted):
        star = star_graph

        center = star.center
        neighbors = star.neighbors
        triplets = star.triplets
        degree = df_degree_broadcasted.get(center, 0)


        results = [
            (triplet, Row(center=center, degree=degree, neighbors=neighbors))
            for triplet in triplets
        ]
        return results
    
    @staticmethod
    def reduce_function(partition: Tuple[str, List], df_degree_broadcasted, n_partitions, lambda_):
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
            hash_center = DynamicInteractions.node2hash(center, n_partitions)

            for neigh_info in neighbors:
                neighbor_id = neigh_info.vertex_id # prima era v
                neighbor_distance = neigh_info.weight

                sumWeight += (1.0 - neighbor_distance)

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
                                    center=center, neighbor=neighbor_id, weight=neighbor_distance
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
            if edge.weight < 1 and edge.weight > 0:
                deg_u = df_degree_broadcasted.get(edge.center, 0)
                deg_v = df_degree_broadcasted.get(edge.neighbor, 0)
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
                result.append(attr)

        return result
