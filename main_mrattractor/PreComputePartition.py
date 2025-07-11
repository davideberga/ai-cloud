"""
Pre-compute partitions of star graphs using PySpark.
A star graph has a center and list of neighbors. 
We need to find G_{ijk} that a star graph belongs to so that we don't need to re-partition 
the star graph times to times.
"""
import logging
from typing import List
from pyspark.sql import SparkSession

class PreComputePartition:
    JOB_NAME = "PreComputePartition"
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.sc = self.spark.sparkContext 
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_edges_from_input(self, input_path: str):
        lines_rdd = self.sc.textFile(input_path)

        def parse_edge_line(line):
            line = line.strip()
            if not line:
                return None
            parts = line.split('\t')
            if len(parts) < 3:
                return None
            try:
                edge_type = parts[0].strip()
                begin_vertex = int(parts[1].strip())
                end_vertex = int(parts[2].strip())
                return (begin_vertex, end_vertex) if edge_type == 'G' else None
            except:
                return None

        edges_rdd = lines_rdd.map(parse_edge_line).filter(lambda x: x is not None)
        print(f"Loaded {edges_rdd.count()} edges of type 'G' from {input_path}")
        return edges_rdd

    def save_results(self, results_rdd, output_path: str):
        def format_result_local(data):
            edge_type, center, list_size, triples_data = data
            triples_str = [f"{l},{m},{r}" for l, m, r in triples_data]
            return f"{edge_type}\t{center}\t{list_size}\t{';'.join(triples_str)}"
        
        formatted_rdd = results_rdd.map(format_result_local)
        formatted_rdd.coalesce(1).saveAsTextFile(output_path)

    def run(self, args: List[str]) -> int:
        try:
            if len(args) < 3:
                raise ValueError("Usage: PreComputePartition <input_path> <output_path> <no_partitions>")

            input_path, output_path, no_partitions = args[0], args[1], int(args[2])

            print(f"Starting job: {self.JOB_NAME}")
            print(f"Input: {input_path}")
            print(f"Output: {output_path}")
            print(f"Partitions: {no_partitions}")

            edges_rdd = self.load_edges_from_input(input_path)

            # Cattura no_partitions in una variabile locale per evitare problemi di serializzazione
            p = no_partitions

            # Definisci tutte le funzioni localmente nel metodo run()
            def node2hash_local(u, no_partitions):
                return u % no_partitions

            def map_function_local(edge_data):
                try:
                    center, target = edge_data
                    u = center
                    v = target
                    
                    hash_u = node2hash_local(u, p)
                    hash_v = node2hash_local(v, p)
                    
                    results = []

                    if hash_u == hash_v:
                        for a in range(p):
                            for b in range(a + 1, p):
                                if a == hash_v or b == hash_v:
                                    continue
                                triple = (a, b, hash_v)
                                results.append((u, triple))
                                results.append((v, triple))
                    else:
                        for a in range(p):
                            if a != hash_u and a != hash_v:
                                triple = (a, hash_u, hash_v)
                                results.append((u, triple))
                                results.append((v, triple))

                    return results
                except Exception as e:
                    raise RuntimeError(f"Error in map_function: {e}")

            def combine_function_local(vertex_triples):
                try:
                    vertex, triples = vertex_triples
                    unique_triples = set(triples)
                    return (vertex, list(unique_triples))
                except Exception as e:
                    raise RuntimeError(f"Error in combine_function: {e}")

            def reduce_function_local(vertex_triples):
                try:
                    vertex, triples = vertex_triples
                    assert len(triples) <= p * p, f"The number of distinct Triplets should be smaller than p*p: {len(triples)} > {p * p}"

                    triples_data = [(t[0], t[1], t[2]) for t in triples]
                    return ('S', vertex, len(triples), triples_data)
                except Exception as e:
                    raise RuntimeError(f"Error in reduce_function: {e}")

            # Applica le trasformazioni usando le funzioni locali
            mapped_rdd = edges_rdd.flatMap(map_function_local)
            grouped_rdd = mapped_rdd.groupByKey()
            combined_rdd = grouped_rdd.map(combine_function_local)
            results_rdd = combined_rdd.map(reduce_function_local)

            self.save_results(results_rdd, output_path)

            print(f"Job completed successfully!")
            return 1

        except Exception as e:
            print(f"Error running job: {e}")
            return 0

    def stop(self):
        self.spark.stop()