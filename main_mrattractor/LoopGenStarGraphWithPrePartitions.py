#!/usr/bin/env python3
"""
Generate star graphs from edges + pre-computed partitions.
Python translation of LoopGenStarGraphWithPrePartitions.java
"""

import sys
import logging
import traceback
from collections import defaultdict
from typing import List, Dict, Tuple, Iterator
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Assumendo che queste classi siano già implementate in altri file
from writable.NeighborWritable import NeighborWritable
from writable.Settings import Settings
from writable.SpecialEdgeTypeWritable import SpecialEdgeTypeWritable
from writable.StarGraphWithPartitionWritable import StarGraphWithPartitionWritable
from writable.StarGraphWritable import StarGraphWritable
from writable.TripleWritable import TripleWritable
from main_mrattractor.MasterMR import MasterMR
from main_mrattractor.AttrUtils import AttrUtils

# Setup logging
logging.basicConfig(level=logging.INFO)
logger_map = logging.getLogger('LoopGenStarGraphWithPrePartitions.Map')
logger_reduce = logging.getLogger('LoopGenStarGraphWithPrePartitions.Reduce')


class LoopGenStarGraphWithPrePartitions:
    """
    Main class for generating star graphs with pre-computed partitions
    """
    
    job_name = "Generate Star Graphs With PreComputed-Partitions."
    global_job = None
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.map_deg = {}
    
    class Mapper:
        """
        Mapper class equivalent to the Java Map class
        Input: Each line is an undirected unweighted edge (u,v)
        """
        
        def __init__(self):
            self.map_deg = {}
        
        def setup(self, context):
            """Setup phase for mapper"""
            pass
        
        def map(self, line_id: SpecialEdgeTypeWritable, null_value, context) -> Iterator[Tuple]:
            """
            Map function that processes each edge
            
            Args:
                line_id: SpecialEdgeTypeWritable containing edge information
                null_value: Null value (not used)
                context: MapReduce context
                
            Yields:
                Tuple of (key, value) pairs for reducer
            """
            try:
                if line_id.type == 'S':  # Star graph type
                    key = line_id.center
                    yield (key, line_id)
                    
                elif line_id.type == 'G':  # Graph/Edge type
                    # Emit for center vertex
                    key = line_id.center
                    value = SpecialEdgeTypeWritable()
                    value.init('G', line_id.center, line_id.target, 
                              line_id.weight, -1, None, -1, None)
                    yield (key, value)
                    
                    # Emit for target vertex (undirected edge)
                    key = line_id.target
                    value = SpecialEdgeTypeWritable()
                    value.init('G', line_id.target, line_id.center, 
                              line_id.weight, -1, None, -1, None)
                    yield (key, value)
                    
                    if MasterMR.DEBUG:
                        print(f"Distance of edge: {line_id.target} {line_id.center}: {line_id.weight}")
                        
            except Exception as e:
                error_msg = f"{MasterMR.prefix_log} {traceback.format_exc()}"
                logger_map.error(error_msg)
                if LoopGenStarGraphWithPrePartitions.global_job:
                    LoopGenStarGraphWithPrePartitions.global_job.kill_job()
                raise
    
    class Reducer:
        """
        Reducer class equivalent to the Java Reduce class
        """
        
        def __init__(self):
            self.multiple_outputs = None
            self.p = 20
            self.map_deg = {}
        
        def setup(self, context):
            """Setup phase for reducer"""
            self.multiple_outputs = context.get_multiple_outputs()
            
            # Read degree map from configuration
            conf = context.get_configuration()
            self.map_deg = AttrUtils.read_deg_map(conf, MasterMR.degree_file_key)
        
        def reduce(self, key: int, values: List[SpecialEdgeTypeWritable], context) -> None:
            """
            Reduce function that processes values for each key
            
            Args:
                key: Center vertex ID
                values: List of SpecialEdgeTypeWritable objects
                context: MapReduce context
            """
            try:
                center = key
                deg_cnt = 0
                count_info_partitions = 0
                neighbors = []
                triples = []
                
                for value in values:
                    assert center == value.center, f"Center mismatch: {center} != {value.center}"
                    
                    if value.type == Settings.STAR_GRAPH:
                        # Partition information
                        count_info_partitions += 1
                        for trip in value.triplet_graphs:
                            new_trip = TripleWritable()
                            new_trip.set(trip.left, trip.mid, trip.right)
                            triples.append(new_trip)
                            
                    elif value.type == Settings.EDGE_TYPE:
                        # Edge information
                        neighbor = NeighborWritable()
                        neighbor.set(value.target, value.weight)
                        neighbors.append(neighbor)
                        deg_cnt += 1
                
                assert count_info_partitions == 1, f"Expected 1 partition info, got {count_info_partitions}"
                
                deg_center = self.map_deg.get(center)
                
                # THEOREM: Star graph with neighbors < true_degree is unnecessary
                # since neighbors are only for exclusive interactions
                if deg_cnt < deg_center:
                    return
                
                # Degree check if enabled
                if MasterMR.check_degree:
                    assert deg_cnt == deg_center, f"degcnt: {deg_cnt}, while key.degnode: {deg_center}"
                
                # Sort neighbors
                neighbors.sort(key=lambda x: x.vertex_id)
                
                # Create and output star graph
                star = StarGraphWithPartitionWritable()
                star.set(center, neighbors, triples)
                
                self.multiple_outputs.write("graph", None, star, "star/star")
                
            except Exception as e:
                error_msg = f"{MasterMR.prefix_log} {traceback.format_exc()}"
                logger_reduce.error(error_msg)
                if LoopGenStarGraphWithPrePartitions.global_job:
                    LoopGenStarGraphWithPrePartitions.global_job.kill_job()
                raise
        
        def cleanup(self, context):
            """Cleanup phase for reducer"""
            if self.multiple_outputs:
                self.multiple_outputs.close()
    
    def run(self, args: List[str]) -> int:
        """
        Main run method equivalent to Java's run method
        
        Args:
            args: Command line arguments
            
        Returns:
            Status code (1 for success)
        """
        try:
            # Parse arguments
            input_files = args[0].split(",")
            output_files = args[1]
            degree_file = args[2]
            
            # Create job configuration
            job_config = {
                'job_name': self.job_name,
                'mapper_class': self.Mapper,
                'reducer_class': self.Reducer,
                'num_reduce_tasks': 40,
                'input_format': 'SequenceFileInputFormat',
                'output_format': 'SequenceFileOutputFormat',
                'map_output_key_class': int,
                'map_output_value_class': SpecialEdgeTypeWritable,
                'output_key_class': type(None),
                'output_value_class': StarGraphWithPartitionWritable,
                'input_paths': [input_files[0], input_files[1]],
                'output_path': output_files,
                'degree_file_key': degree_file
            }
            
            # Set global job reference
            LoopGenStarGraphWithPrePartitions.global_job = self
            
            # Configure multiple outputs
            self._add_named_output("graph", "SequenceFileOutputFormat", 
                                 type(None), StarGraphWithPartitionWritable)
            
            # Delete output directory if exists
            self._delete_path(output_files)
            
            # Wait for job completion
            success = self._wait_for_completion(job_config)
            
            return 1 if success else 0
            
        except Exception as e:
            logger_reduce.error(f"Job execution failed: {traceback.format_exc()}")
            return 0
    
    def _add_named_output(self, name: str, format_class: str, 
                         key_class: type, value_class: type):
        """Add named output for multiple outputs"""
        # Implementation would depend on the Python MapReduce framework being used
        pass
    
    def _delete_path(self, path: str):
        """Delete output path if it exists"""
        # Implementation would depend on the file system being used
        pass
    
    def _wait_for_completion(self, job_config: Dict) -> bool:
        """Wait for job completion"""
        # Implementation would depend on the MapReduce framework being used
        return True
    
    def kill_job(self):
        """Kill the current job"""
        # Implementation would depend on the MapReduce framework being used
        pass


def main():
    """Main entry point"""
    if len(sys.argv) < 4:
        print("Usage: python loop_gen_star_graph_with_pre_partitions.py <input_files> <output_path> <degree_file>")
        sys.exit(1)
    
    tool = LoopGenStarGraphWithPrePartitions()
    result = tool.run(sys.argv[1:])
    sys.exit(result)


if __name__ == "__main__":
    main()