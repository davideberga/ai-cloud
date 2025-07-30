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
    def aggregate_comb(acc: List, b):
        acc.append(b)
        return acc

    @staticmethod
    def mapReduce(df_graph_jaccard, df_partitioned, df_graph_degree):
        df_graph_jaccard = df_graph_jaccard.flatMap(
            MRStarGraphWithPrePartitions.both_directions
        )
        rdd_mapped = df_graph_jaccard.union(df_partitioned).groupByKey()
        result_rdd = rdd_mapped.flatMap(
            lambda a: MRStarGraphWithPrePartitions.reduce_function(
                a, df_graph_degree.value
            )
        )
        return result_rdd

    @staticmethod
    def both_directions(row):
        center = int(row[0].split("-")[0])
        type_, target, weight, _ = row[1]
        return [(center, (type_, target, weight)), (target, (type_, center, weight))]

    # input: vertex_id is the id of the certal node of the star graph
    # output: list of rows with center, neighbors, and triplets
    @staticmethod
    def reduce_function(a, df_graph_degree):
        vertex_id, entries = a

        deg_center = df_graph_degree.get(vertex_id, 0)
        neighbors = []
        triplets = []
        for entry in entries:
            if entry[0] == "S":
                triplets.extend(entry[1])
            elif entry[0] == "G":
                type_, target, weight = entry
                neighbors.append((target, weight))

        if len(neighbors) < deg_center:
            return []

        # sort neighbors by vertex_id
        sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
        neighbors_row = [Row(vertex_id=vid, weight=w) for vid, w in sorted_neighbors]

        return [Row(center=vertex_id, neighbors=neighbors_row, triplets=triplets)]
