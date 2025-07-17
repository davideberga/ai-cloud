"""
Generate star graphs from edges + pre-computed partitions.
PySpark implementation of LoopGenStarGraphWithPrePartitions
"""
import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('LoopGenStarGraphWithPrePartitions.PySpark')

class MRStarGraphWithPrePartitions:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.logger = logging.getLogger(self.__class__.__name__)

    def mapReduce(self, df_graph_jaccard, df_partitioned, df_graph_degree):
        
        print("Start Star Graph Computation")
        
        # input: can be either df_graph_jaccard or df_partitioned
        # output: represents edges or partition info associated with each vertex
        def map_function(row):
            results = []
            if row[0] == 'S':
                # This is partition info
                results.append((row[1], {'type': 'S', 'triplets': row[3]}))
            elif row.edge_type == 'G':
                # For each edge, emit both directions
                results.append((row.vertex_start, {'type': 'G', 'target': row.vertex_end, 'weight': row.distance}))
                results.append((row.vertex_end, {'type': 'G', 'target': row.vertex_start, 'weight': row.distance}))
            return results
        
        jaccard_mapped = df_graph_jaccard.flatMap(map_function)
        partitions_mapped = df_partitioned.flatMap(map_function)
        rdd_mapped = jaccard_mapped.union(partitions_mapped)

        # input: vertex_id is the id of the certal node of the star graph
        # output: list of rows with center, neighbors, and triplets
        def reduce_function(a):
            vertex_id, entries = a
            deg_center = df_graph_degree.value.get(vertex_id, 0)
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
        
        
        result_rdd = rdd_mapped.groupByKey().flatMap(reduce_function)

        print("Start Star Graph Computation END")
        
        return result_rdd
