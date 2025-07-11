"""
Generate star graphs from edges + pre-computed partitions.
PySpark implementation of LoopGenStarGraphWithPrePartitions
"""
import logging
import traceback
from collections import namedtuple
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('LoopGenStarGraphWithPrePartitions.PySpark')

# Define simple data structures to replace Writable classes
Neighbor = namedtuple('Neighbor', ['vertex_id', 'weight'])
Triple = namedtuple('Triple', ['left', 'mid', 'right'])
SpecialEdgeType = namedtuple('SpecialEdgeType', ['type', 'center', 'target', 'weight', 'triplet_graphs'])
StarGraphWithPartition = namedtuple('StarGraphWithPartition', ['center', 'neighbors', 'triples'])

# Constants to replace Settings class
STAR_GRAPH = 'S'
EDGE_TYPE = 'G'

# Define parsing function outside of class to avoid serialization issues
def parse_text_line(line):
    """
    Parse text line to SpecialEdgeType
    Assuming format: type\tcenter\ttarget\tweight\t[triplet_data]
    """
    try:
        parts = line.strip().split('\t')
        if len(parts) < 3:
            return None
            
        edge_type = parts[0].strip()
        center = int(parts[1])
        
        if edge_type == STAR_GRAPH:
            # Star graph format: S\tcenter\tnum_triples\ttriple1,triple2,...
            if len(parts) >= 4:
                num_triples = int(parts[2])
                triplet_graphs = []
                if len(parts) > 3 and parts[3]:
                    triple_strs = parts[3].split(';')
                    for triple_str in triple_strs:
                        if triple_str:
                            left, mid, right = map(int, triple_str.split(','))
                            triplet_graphs.append(Triple(left, mid, right))
                
                return SpecialEdgeType(
                    type=STAR_GRAPH,
                    center=center,
                    target=-1,
                    weight=0.0,
                    triplet_graphs=triplet_graphs
                )
                
        elif edge_type == EDGE_TYPE:
            # Edge format: G\tcenter\ttarget\tweight
            if len(parts) >= 4:
                target = int(parts[2])
                weight = float(parts[3])
                
                return SpecialEdgeType(
                    type=EDGE_TYPE,
                    center=center,
                    target=target,
                    weight=weight,
                    triplet_graphs=[]
                )
        
        return None
        
    except Exception as e:
        logger.error(f"Error parsing line: {line}, error: {e}")
        return None


# Define map function at module level to avoid serialization issues
def map_function_global(line_id):
    """
    Map function that processes each edge - PySpark version
    """
    try:
        results = []
        
        if line_id.type == STAR_GRAPH:  # Star graph type
            key = line_id.center
            results.append((key, line_id))
            
        elif line_id.type == EDGE_TYPE:  # Graph/Edge type
            # Emit for center vertex
            key = line_id.center
            value = SpecialEdgeType(
                type=EDGE_TYPE,
                center=line_id.center,
                target=line_id.target,
                weight=line_id.weight,
                triplet_graphs=[]
            )
            results.append((key, value))
            
            # Emit for target vertex (undirected edge)
            key = line_id.target
            value = SpecialEdgeType(
                type=EDGE_TYPE,
                center=line_id.target,
                target=line_id.center,
                weight=line_id.weight,
                triplet_graphs=[]
            )
            results.append((key, value))
            
        return results
        
    except Exception as e:
        error_msg = f"[STAR_GRAPH_GEN] Map Error: {traceback.format_exc()}"
        print(error_msg)  # Use print instead of logger in worker
        raise


# Define reduce function at module level to avoid serialization issues
def reduce_function_global(key_values):
    """
    Reduce function that processes values for each key - PySpark version
    """
    try:
        key, values = key_values
        center = key
        deg_cnt = 0
        count_info_partitions = 0
        neighbors = []
        triples = []
        
        # Convert values to list if it's an iterator
        if hasattr(values, '__iter__') and not isinstance(values, list):
            values = list(values)
        
        for value in values:
            assert center == value.center, f"Center mismatch: {center} != {value.center}"
            
            if value.type == STAR_GRAPH:
                # Partition information
                count_info_partitions += 1
                for trip in value.triplet_graphs:
                    triples.append(Triple(trip.left, trip.mid, trip.right))
                    
            elif value.type == EDGE_TYPE:
                # Edge information
                neighbor = Neighbor(value.target, value.weight)
                neighbors.append(neighbor)
                deg_cnt += 1
        
        # Check if we have partition information
        if count_info_partitions == 0:
            # Skip if no partition info
            return []
        
        assert count_info_partitions == 1, f"Expected 1 partition info, got {count_info_partitions}"
        
        # Sort neighbors by vertex_id
        neighbors.sort(key=lambda x: x.vertex_id)
        
        # Create and return star graph
        star = StarGraphWithPartition(
            center=center,
            neighbors=neighbors,
            triples=triples
        )
        
        return [star]
        
    except Exception as e:
        error_msg = f"[STAR_GRAPH_GEN] Reduce Error: {traceback.format_exc()}"
        print(error_msg)  # Use print instead of logger in worker
        raise


# Define format function at module level
def format_star_graph(star):
    """Format star graph for output"""
    center = star.center
    neighbors_str = ','.join([f"{n.vertex_id}:{n.weight}" for n in star.neighbors])
    triples_str = ';'.join([f"{t.left},{t.mid},{t.right}" for t in star.triples])
    return f"{center}\t{neighbors_str}\t{triples_str}"


class LoopGenStarGraphWithPrePartitions:
    """
    PySpark implementation for generating star graphs with pre-computed partitions
    """
    
    job_name = "Generate Star Graphs With PreComputed-Partitions (PySpark)"
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
        self.map_deg = {}
        self.DEBUG = False
        self.check_degree = False
        self.prefix_log = "[STAR_GRAPH_GEN]"
    
    def _load_degree_file(self, degree_file: str) -> Dict[int, int]:
        """
        Load degree file using PySpark
        
        Args:
            degree_file: Path to degree file
            
        Returns:
            Dictionary mapping vertex_id to degree
        """
        try:
            # Read degree file as RDD
            degree_rdd = self.sc.textFile(degree_file)
            
            # Parse degree file (assuming format: vertex_id,degree)
            def parse_degree_line(line):
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        vertex_id = int(parts[0])
                        degree = int(parts[1])
                        return (vertex_id, degree)
                    except ValueError:
                        # Skip lines that can't be parsed as integers
                        return None
                return None
            
            degree_pairs = degree_rdd.map(parse_degree_line).filter(lambda x: x is not None)
            degree_dict = degree_pairs.collectAsMap()
            
            logger.info(f"Loaded {len(degree_dict)} degree entries from {degree_file}")
            return degree_dict
            
        except Exception as e:
            logger.error(f"Error loading degree file {degree_file}: {traceback.format_exc()}")
            raise
    
    def _read_input_data(self, input_files: List[str]):
        """
        Read input data from files
        
        Args:
            input_files: List of input file paths
            
        Returns:
            RDD of SpecialEdgeType objects
        """
        try:
            all_rdds = []
            for input_file in input_files:
                # Read as text file and parse - use the global function
                rdd = self.sc.textFile(input_file)
                rdd = rdd.map(parse_text_line).filter(lambda x: x is not None)
                all_rdds.append(rdd)
            
            # Union all RDDs
            if len(all_rdds) == 1:
                return all_rdds[0]
            else:
                return self.sc.union(all_rdds)
                
        except Exception as e:
            logger.error(f"Error reading input data: {traceback.format_exc()}")
            raise
    
    def _save_output(self, star_graphs_rdd, output_path: str):
        """
        Save star graphs to output path
        
        Args:
            star_graphs_rdd: RDD of StarGraphWithPartition objects
            output_path: Output path
        """
        try:
            # Create output directory structure
            star_output_path = f"{output_path}/star"
            
            # Convert to string format for saving using global function
            formatted_rdd = star_graphs_rdd.map(format_star_graph)
            formatted_rdd.saveAsTextFile(star_output_path)
            
            logger.info(f"Star graphs saved to {star_output_path}")
            
        except Exception as e:
            logger.error(f"Error saving output: {traceback.format_exc()}")
            raise
    
    def run(self, args: List[str]) -> int:
        """
        Main run method - PySpark implementation
        
        Args:
            args: Command line arguments [input_files, output_path, degree_file]
            
        Returns:
            Status code (1 for success, 0 for failure)
        """
        try:
            # Parse arguments
            input_files = args[0].split(",")
            output_path = args[1]
            degree_file = args[2]
            
            logger.info(f"Starting {self.job_name}")
            logger.info(f"Input files: {input_files}")
            logger.info(f"Output path: {output_path}")
            logger.info(f"Degree file: {degree_file}")
            
            # Load degree file
            self.map_deg = self._load_degree_file(degree_file)
            
            # Read input data
            input_rdd = self._read_input_data(input_files)
            
            # Apply map function - flatMap to handle multiple outputs per input
            mapped_rdd = input_rdd.flatMap(map_function_global)
            
            # Group by key (equivalent to shuffle phase in MapReduce)
            grouped_rdd = mapped_rdd.groupByKey()
            
            # Apply reduce function
            star_graphs_rdd = grouped_rdd.flatMap(reduce_function_global)
            
            # Cache the result for potential reuse
            star_graphs_rdd.cache()
            
            # Count for logging
            star_count = star_graphs_rdd.count()
            logger.info(f"Generated {star_count} star graphs")
            
            # Save output
            self._save_output(star_graphs_rdd, output_path)
            
            logger.info(f"Job completed successfully")
            return 1
            
        except Exception as e:
            logger.error(f"Job execution failed: {traceback.format_exc()}")
            return 0
    
    def kill_job(self):
        """Kill the current job"""
        try:
            self.sc.stop()
        except:
            pass