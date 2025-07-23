"""
Generate star graphs from edges + pre-computed partitions.
PySpark implementation of LoopGenStarGraphWithPrePartitions
"""

from typing import List
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *


class MRStarGraphWithPrePartitions:
    @staticmethod
    def aggregate_seq(acc: List, b: dict):
        acc.extend([b])
        return acc

    @staticmethod
    def aggregate_comb(acc: List, b: List):
        acc.extend(b)
        return acc

    @staticmethod
    def mapReduce(df_graph_jaccard, df_partitioned, df_graph_degree):
        rdd_mapped = df_graph_jaccard.union(df_partitioned)
        rdd_mapped = rdd_mapped.reduceByKey(MRStarGraphWithPrePartitions.aggregate_comb)
        
        print(rdd_mapped.take(5))
        exit(0)

        result_rdd = rdd_mapped.flatMap(
            lambda a: MRStarGraphWithPrePartitions.reduce_function(
                a, df_graph_degree.value
            )
        )

        return result_rdd

    # input: vertex_id is the id of the certal node of the star graph
    # output: list of rows with center, neighbors, and triplets
    @staticmethod
    def reduce_function(a, df_graph_degree):
        vertex_id, entries = a
        
        deg_center = df_graph_degree.get(vertex_id, 0)
        neighbors = []
        triplets = []
        for entry in entries:
            if entry["type"] == "S":
                triplets.extend(entry["triplets"])
            elif entry["type"] == "G":
                neighbors.append((entry["target"], entry["weight"]))

        if len(neighbors) < deg_center:
            return []

        # sort neighbors by vertex_id
        sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
        neighbors_row = [Row(vertex_id=vid, weight=w) for vid, w in sorted_neighbors]

        return [Row(center=vertex_id, neighbors=neighbors_row, triplets=triplets)]
