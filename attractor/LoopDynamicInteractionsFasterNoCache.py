from pyspark.sql import DataFrame
from pyspark import RDD
from pyspark.sql import SparkSession
import logging
from typing import List, Dict, Tuple
import pickle
from collections import defaultdict

class LoopDynamicInteractionsFasterNoCache:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.logger = logging.getLogger("LoopDynamicInteractions")

    def compute(self, inputs: List):
        # Unpack inputs
        df_star_graph, no_partition_dynamic_interaction, lambda_val, df_graph_degree = inputs

        # Step 1: Broadcast degree map
        map_deg = self._get_degree_map(df_graph_degree)
        bcast_deg = self.sc.broadcast(map_deg)

        # Step 2: Convert DataFrame to RDD
        star_graph_rdd = df_star_graph.rdd.map(self._deserialize_star_graph)

        # Step 3: FlatMap (equivalente al mapper)
        intermediate_rdd = star_graph_rdd.flatMap(
            lambda star: self.map_function(star, bcast_deg.value)
        )

        # Step 4: GroupByKey to simulate shuffle by TripleWritable key
        grouped_rdd = intermediate_rdd.groupByKey()

        # Step 5: Reduce (simula la reduce di Hadoop)
        result_rdd = grouped_rdd.flatMap(
            lambda kv: self.reduce_function(kv[0], kv[1], lambda_val, bcast_deg.value)
        )

        return result_rdd

    def map_function(self, star_graph, map_deg: Dict[int, int]):
        results = []
        center = star_graph["center"]
        degree = map_deg.get(center, star_graph["degree"])  # fallback if missing
        neighbors = star_graph["neighbors"]
        star_data = {
            "center": center,
            "deg": degree,
            "neighbors": neighbors
        }

        for triplet in star_graph["tripleSubGraphs"]:
            results.append((tuple(triplet), star_data))

        return results

    def reduce_function(self, triplet_key: Tuple[int, int, int], stars, lambda_val: float, map_deg: Dict[int, int]):
        components = set(triplet_key)
        adj_main = defaultdict(list)
        adj_full = defaultdict(list)
        sum_weights = {}
        edges = []

        # Parse stars and build adjacency
        for star in stars:
            center = star["center"]
            deg_center = star["deg"]
            neighbors = star["neighbors"]

            sum_w = 0
            for n in neighbors:
                neighbor = n["lab"]
                dis = n["dis"]
                sum_w += (1 - dis)

                adj_full[center].append(n)

                if self.is_main_node(neighbor, components):
                    adj_main[center].append(n)
                    if center > neighbor and 0 < dis < 1:
                        edges.append((center, neighbor, dis))

            sum_weights[center] = sum_w

        # Reduce logic: compute dynamic interactions per edge
        output = []
        for u, v, dis in edges:
            deg_u = map_deg.get(u, 1)
            deg_v = map_deg.get(v, 1)
            result = self._union_intersection(
                u, v, deg_u, deg_v, adj_main, adj_full, sum_weights, triplet_key, lambda_val
            )
            output.extend(result)

        return output
    
    def is_main_node(self, node_id: int, components: set):
        return (node_id % len(components)) in components    
        
