"""
Generate star graphs from edges + pre-computed partitions.
PySpark implementation of LoopGenStarGraphWithPrePartitions
"""
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

class MRStarGraphWithPrePartitions:
    @staticmethod
    def mapReduce(df_graph_jaccard, df_partitioned, df_graph_degree):
        
        print("Start Star Graph Computation")
        
        jaccard_mapped = df_graph_jaccard.flatMap(MRStarGraphWithPrePartitions.map_function)
        partitions_mapped = df_partitioned.flatMap(MRStarGraphWithPrePartitions.map_function)
        rdd_mapped = jaccard_mapped.union(partitions_mapped)

        result_rdd = rdd_mapped.groupByKey().flatMap(
            lambda a: MRStarGraphWithPrePartitions.reduce_function(a, df_graph_degree.value)
        )

        print("Start Star Graph Computation END")
        
        return result_rdd
    
    # input: can be either df_graph_jaccard or df_partitioned
    # output: represents edges or partition info associated with each vertex
    @staticmethod
    def map_function(row):
        results = []
        if row[0] == 'S':
            # This is partition info
            results.append((row[1], {'type': 'S', 'triplets': row[3]}))
        elif row.type == 'G':
            # For each edge, emit both directions
            results.append((row.center, {'type': 'G', 'target': row.target, 'weight': row.weight}))
            results.append((row.target, {'type': 'G', 'target': row.center, 'weight': row.weight}))
        return results
    
    # input: vertex_id is the id of the certal node of the star graph
    # output: list of rows with center, neighbors, and triplets
    @staticmethod
    def reduce_function(a, df_graph_degree):
        vertex_id, entries = a
        deg_center = df_graph_degree.get(vertex_id, 0)
        neighbors = []
        triplets = []
        for entry in entries:
            if entry['type'] == 'S':
                triplets.extend(entry['triplets'])
            elif entry['type'] == 'G':
                neighbors.append((entry['target'], entry['weight']))

        if len(neighbors) < deg_center:
            return []

        # sort neighbors by vertex_id
        sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
        neighbors_row = [Row(vertex_id=vid, weight=w) for vid, w in sorted_neighbors]

        return [Row(center=vertex_id, neighbors=neighbors_row, triplets=triplets)]
