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

class LoopGenStarGraphWithPrePartitions:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.logger = logging.getLogger(self.__class__.__name__)

    def compute(self, df_graph_jaccard, df_partitioned, df_graph_degree):
        
        # input: can be either df_graph_jaccard or df_partitioned
        # output: represents edges or partition info associated with each vertex
        def map_function(row):
            results = []
            if row['edge_type'] == 'S':
                # This is partition info
                results.append((row.vertex_id, {'type': 'S', 'triplets': row.triplets}))
            elif row['edge_type'] == 'G':
                # For each edge, emit both directions
                results.append((row.vertex_start, {'type': 'G', 'target': row.vertex_end, 'weight': row.distance}))
                results.append((row.vertex_end, {'type': 'G', 'target': row.vertex_start, 'weight': row.distance}))
            return results

        rdd_mapped = (
            df_graph_jaccard.rdd.flatMap(map_function)
            .union(df_partitioned.rdd.flatMap(map_function))
        )

        deg_map = {
            row.vertex_id: row.degree for row in df_graph_degree.collect()
        }
        # Broadcast the degree map for efficient access in the reducer
        broadcast_deg_map = self.spark.sparkContext.broadcast(deg_map)

        # input: vertex_id is the id of the certal node of the star graph
        # output: list of rows with center, neighbors, and triplets
        def reduce_function(vertex_id, entries):
            deg_map = broadcast_deg_map.value
            neighbors = []
            triplets = []
            for entry in entries:
                if entry['type'] == 'S':
                    triplets.extend(entry['triplets'])
                elif entry['type'] == 'G':
                    neighbors.append((entry['target'], entry['weight']))

            deg_center = deg_map.get(vertex_id, 0)
            if len(neighbors) < deg_center:
                return []

            # sort neighbors by vertex_id
            sorted_neighbors = sorted(neighbors, key=lambda x: x[0])
            neighbors_row = [Row(vertex_id=vid, weight=w) for vid, w in sorted_neighbors]

            return [Row(center=vertex_id, neighbors=neighbors_row, triplets=triplets)]

        grouped_rdd = rdd_mapped.groupByKey().mapValues(list)
        result_rdd = grouped_rdd.flatMap(lambda kv: reduce_function(kv[0], kv[1]))

        return result_rdd
