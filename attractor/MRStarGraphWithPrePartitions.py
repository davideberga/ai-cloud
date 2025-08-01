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
        target, weight, sliding, degree_center, degree_target, partitions_center, partitions_neighbour = row[1]
        return [
            (center, (target, weight, sliding, degree_center, degree_target, partitions_center)),
            (target, (center, weight, sliding, degree_target, degree_center, partitions_neighbour)),
        ]

    # input: vertex_id is the id of the certal node of the star graph
    # output: list of rows with center, neighbors, and triplets
    @staticmethod
    def reduce_function(a):
        vertex_id, entries = a

        neighbors = []
        triplets = []
        for entry in entries:
            target, weight, sliding, degree, degree_neigh, partitions = entry
            triplets.extend(partitions)
            neighbors.append((target, weight, degree_neigh, sliding))
                

        if len(neighbors) < degree:
            return []

        # sort neighbors by vertex_id
        sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
        final_neighbors = []
        final_sliding = []
        for n_id, w, deg, sliding in sorted_neighbors:
            final_neighbors.append((n_id, w, deg))
            final_sliding.append((f"{vertex_id}-{n_id}", sliding))
        
        seen = set()
        partitions = []
        for s in triplets:
            nums = sorted(s.split(), key=int)  
            sorted_s = " ".join(nums)          
            key = frozenset(nums)              
            if key not in seen:
                seen.add(key)
                partitions.append(sorted_s) 
                
        res =  [
            (triplet, (vertex_id, degree, final_neighbors))
            for triplet in partitions
        ]      
        
        res.extend(final_sliding)
        return res 
