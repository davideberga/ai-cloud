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
    def mapReduce(df_graph_jaccard):
        df_graph_jaccard = df_graph_jaccard.flatMap(
            MRStarGraphWithPrePartitions.both_directions
        ).groupByKey()
        result_rdd = df_graph_jaccard.flatMap(MRStarGraphWithPrePartitions.reduce_function)
        return result_rdd

    @staticmethod
    def both_directions(row):
        center = int(row[0].split("-")[0])
        type_, target, weight, sliding, degree_center, degree_target, partitions_center, partitions_neighbour = row[1]
        return [
            (center, (type_, target, weight, sliding, degree_center, degree_target, partitions_center)),
            (target, (type_, center, weight, sliding, degree_target, degree_center, partitions_neighbour)),
        ]

    # input: vertex_id is the id of the certal node of the star graph
    # output: list of rows with center, neighbors, and triplets
    @staticmethod
    def reduce_function(a):
        vertex_id, entries = a

        neighbors = []
        triplets = []
        for entry in entries:
            type_, target, weight, sliding, degree, degree_neigh, partitions = entry
            triplets.extend(partitions)
            neighbors.append((target, weight, degree_neigh, sliding))
                

        if len(neighbors) < degree:
            return []

        # sort neighbors by vertex_id
        sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
        neighbors_row = [
            Row(vertex_id=vid, weight=w, degree=deg, sliding=sliding) for vid, w, deg, sliding in sorted_neighbors
        ]
        
        seen = set()
        result = []
        for s in triplets:
            nums = sorted(s.split(), key=int)  # sort numerically
            sorted_s = " ".join(nums)          # reconstruct string
            key = frozenset(nums)              # order-independent representation
            if key not in seen:
                seen.add(key)
                result.append(sorted_s)        # append the sorted string

        return [
            Row(
                center=vertex_id,
                neighbors=neighbors_row,
                triplets=tuple(result),
                degree=degree,
            )
        ]
