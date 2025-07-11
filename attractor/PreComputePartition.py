"""
Pre-compute partitions of star graphs using PySpark.
A star graph has a center and list of neighbors.
We need to find G_{ijk} that a star graph belongs to so that we don't need to re-partition
the star graph times to times.
"""

import logging
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class PreComputePartition:
    JOB_NAME = "PreComputePartition"

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.sc = self.spark.sparkContext
        self.logger = logging.getLogger(self.__class__.__name__)
        
    

    def compute(self, df_edge_with_jaccard: DataFrame, n_partitions: int) -> int:
        print("Starting pre computation of partitions")
        
        rdd_edge_with_jaccard = df_edge_with_jaccard.rdd
        
        def node_to_hash(u: int, no_partitions: int):
            return u % no_partitions
        
        n_partitions = int(n_partitions)

        def map_function_local(edge_data):

            vertex_start, vertex_end  = edge_data.vertex_start, edge_data.vertex_end
            hash_u = node_to_hash(vertex_start, n_partitions)
            hash_v = node_to_hash(vertex_end, n_partitions)
            
            results = []

            if hash_u == hash_v:
                for a in range(n_partitions):
                    for b in range(a + 1, n_partitions):
                        if a == hash_v or b == hash_v:
                            continue
                        triple = (a, b, hash_v)
                        results.append((vertex_start, triple))
                        results.append((vertex_end, triple))
            else:
                for a in range(n_partitions):
                    if a != hash_u and a != hash_v:
                        triple = (a, hash_u, hash_v)
                        results.append((vertex_start, triple))
                        results.append((vertex_end, triple))

            return results


        def reduce_triples(vertex, triples):
            triples_data = []
            for t in triples:
                stringified = t[0] + " " + t[1] + " " + t[2]
                triples_data.append(stringified)
                
            return ("S", vertex, len(triples), triples_data)
        
        map_output = rdd_edge_with_jaccard.flatMap(map_function_local).groupByKey()
        # Dataframe with vertex_id, triple=(i,j,k)
        


        # Applica le trasformazioni usando le funzioni locali
        mapped_rdd = rdd_edge_with_jaccard.flatMap(map_function_local)
        grouped_rdd = mapped_rdd.groupByKey()
        combined_rdd = grouped_rdd.map(combine_function_local)
        results_rdd = combined_rdd.map(reduce_function_local)

        print("Pre computation of partitions: END")
        
        return results_rdd
