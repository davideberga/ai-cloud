"""
A star graph has a center and list of neighbors.
We need to find G_{ijk} that a star graph belongs to so that we don't need to re-partition
the star graph times to times.
"""
from typing import List, Tuple
from pyspark.sql.dataframe import DataFrame
from attractor.DynamicInteractions import DynamicInteractions

class MRPreComputePartition:
    JOB_NAME = "PreComputePartition"

    @staticmethod  
    def mapReduce(rdd_edge_with_jaccard: DataFrame, n_partitions: int) -> int:
        print("Starting pre computation of partitions")
        
        n_partitions = int(n_partitions)

        map_output = rdd_edge_with_jaccard.flatMap(lambda edge: MRPreComputePartition.map_function(edge, n_partitions)).groupByKey()  # Dataframe with vertex_id, triple=(i,j,k)
        combined_output = map_output.mapValues(set)  # Dataframe with vertex_id, set of triples
        reduce_output = combined_output.map(MRPreComputePartition.reduce_function)  # Dataframe with ("S", vertex_id, len(triples), triples) where vertex_id is the center of the star graph in the next step

        print("Pre computation of partitions: END")
        
        return reduce_output
    
    # input: edge(u,v)
    # output: (u, (i, j, k)) (v, (i, j, k)) where i,j,k are partitions
    @staticmethod
    def map_function(edge_data, n_partitions) -> List[Tuple[int, Tuple[int, int, int]]]:

        u, v  = edge_data.center, edge_data.target
        hash_u = DynamicInteractions.node2hash(u, n_partitions)
        hash_v = DynamicInteractions.node2hash(v, n_partitions)
        
        results = []

        # Infra partition edges
        if hash_u == hash_v:
            for a in range(n_partitions):
                for b in range(a + 1, n_partitions):
                    if a == hash_v or b == hash_v:
                        continue
                    triple = tuple(sorted((a, b, hash_v)))
                    results.append((u, triple))
                    results.append((v, triple))
        
        # Inter partition edges
        else:
            for a in range(n_partitions):
                if a != hash_u and a != hash_v:
                    triple = tuple(sorted((a, hash_u, hash_v)))
                    results.append((u, triple))
                    results.append((v, triple))

        return results
    
    # To create the rappresentation of the star graph
    # input: (u, {(i, j, k), ...})
    # output: ("S", u, len(triples), triples)
    @staticmethod
    def reduce_function(vertex_triples_set: Tuple[int, Tuple[int, int, int]]):
        vertex, triples = vertex_triples_set
        triples_data = []
        for (i,j,k) in triples:
            triple_string = f"{i} {j} {k}"
            triples_data.append(triple_string)

        return ("S", vertex, len(triples_data), triples_data)
