import os
import math
from typing import Dict, Set, List, Optional
from single_attractor.Graph import Graph  # Assuming Graph class is implemented in graph.py
from single_attractor.Helper import Helper  # Assuming Helper class is implemented in helper.py
from single_attractor.Settings import Settings  # Assuming Settings class is implemented in settings.py
from single_attractor.VertexValue import VertexValue  # Assuming VertexValue class is implemented in vertex_value.py
from single_attractor.EdgeValue import EdgeValue  # Assuming EdgeValue class is implemented in edge_value.py

class CommunityDetection:
    def __init__(self, sliding_window: int, threshold_sliding_windows: float, 
                 lambda_val: float, cache_size_single_attractor: int, 
                 current_loop: int, original_vertices: int, 
                 out_file_name: str, log_job_master):
        """
        Initialize CommunityDetection with parameters.
        
        Args:
            sliding_window: Size of sliding window
            threshold_sliding_windows: Threshold for sliding windows
            lambda_val: Lambda parameter
            cache_size_single_attractor: Cache size for single attractor
            current_loop: Current loop number
            original_vertices: Number of original vertices
            out_file_name: Output file name
            log_job_master: Logger for master job
        """
        Settings.SLIDING_WINDOW_SIZE = sliding_window
        Settings.DEFAULT_SUPPORT_SLIDING_WINDOW = threshold_sliding_windows
        Settings.lambda_val = lambda_val
        Settings.LIMIT_SIZE_DICT_VIRTUAL_EDGES = cache_size_single_attractor
        
        self.m_cGraph: Optional[Graph] = None
        self.m_iCurrentStep = 0
        self.m_dictVirtualEdgeTempResult: Dict[str, float] = {}
        self.m_dictInteration: Dict[int, int] = {}
        
        self.no_vertices_reduced_graph = 0
        self.no_edges_reduced_graph = 0
        self.current_loops = current_loop
        self.no_vertices_original = original_vertices
        self.outputfile = out_file_name
        self.logSingle = None
        self.logMaster = log_job_master

    def setup_graph(self, str_filename: str) -> None:
        """
        Adding edges to the graph, build dictionary of edges and dictionary of vertices.
        
        Args:
            str_filename: Path to the input file
        """
        self.m_cGraph = Graph()
        self.m_dictVirtualEdgeTempResult = {}
        self.m_dictInteration = {}
        
        with open(str_filename, 'r') as reader:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split()
                i_begin = int(parts[0])
                i_end = int(parts[1])
                # Distance of the edge
                d_weight = float(parts[2])
                
                self.m_cGraph.add_edge(i_begin, i_end, d_weight)
                self.no_edges_reduced_graph += 1
        
        assert self.no_edges_reduced_graph == len(self.m_cGraph.m_dictEdges)
        self.no_vertices_reduced_graph = len(self.m_cGraph.m_dictVertices)
        
        # Debug section (simplified - full implementation would need MasterMR.DEBUG equivalent)
        # ... debug code omitted for brevity

    def initialize_graph(self) -> None:
        """
        Pre-compute the common neighbors and exclusive neighbors of every non-converged edges.
        Pre-compute sumWeight of every node in G(V,E)
        """
        p_edges = self.m_cGraph.get_all_edges()
        cnt_check_sum_weight = 0
        
        for edge_key, p_edge_value in p_edges.items():
            parts = edge_key.split()
            i_begin = int(parts[0])
            i_end = int(parts[1])
            
            # Debug check for specific edge
            if i_begin == 2604 and i_end == 2601:
                pass  # Debug breakpoint equivalent
            
            # We need to find the sumWeight of a star graph whatever the edge weight is
            d_distance = p_edge_value.distance
            self.m_cGraph.update_edge(i_begin, i_end, d_distance, self.m_iCurrentStep)
            self.m_cGraph.add_vertex_weight(i_begin, d_distance, self.m_iCurrentStep)
            self.m_cGraph.add_vertex_weight(i_end, d_distance, self.m_iCurrentStep)
            
            cnt_check_sum_weight += 1
            
            if 0 < p_edge_value.distance < 1:
                # We only consider non-converged edges to avoid out-of-memory issue
                self.compute_common_neighbour(i_begin, i_end, p_edge_value)
                self.compute_exclusive_neighbour(i_begin, i_end, p_edge_value)
        
        self.logMaster.write("Done finding common neighbors and exclusive neighbors\n")
        self.logMaster.flush()
        assert cnt_check_sum_weight == self.no_edges_reduced_graph

    def update_sliding_window(self, merged_sliding_windows_file: str) -> None:
        """
        Updating sliding window from MR version.
        
        Args:
            merged_sliding_windows_file: Path to merged sliding windows file
        """
        if not merged_sliding_windows_file:
            return
        
        no_loops_mr = min(self.current_loops + 1, Settings.SLIDING_WINDOW_SIZE)
        
        with open(merged_sliding_windows_file, 'r') as reader:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                    
                args = line.split()
                u = int(args[0])
                v = int(args[1])
                edge_key = Graph.refine_edge_key(u, v)
                
                if edge_key in self.m_cGraph.m_dictEdges:
                    vl = self.m_cGraph.m_dictEdges[edge_key]
                    status = []
                    for j in range(3, len(args)):
                        s = int(args[j])
                        assert s == 0 or s == 1
                        status.append(s)
                    vl.set_sliding_window(no_loops_mr, status)
        
        # Delete the file after processing
        if os.path.exists(merged_sliding_windows_file):
            os.remove(merged_sliding_windows_file)

    def dynamic_interaction(self) -> None:
        """
        Dynamic Interaction main loop.
        """
        b_continue = True
        p_edges = self.m_cGraph.get_all_edges()
        
        i = 0
        cnt_loop = self.current_loops
        loop_single = 0
        
        while b_continue:
            b_continue = False
            i_next_step = Helper.next_step(self.m_iCurrentStep)
            print(f"Single Machine Current Loop: {cnt_loop + 1}")
            
            import time
            tic = time.time() * 1000  # Convert to milliseconds
            i_converge_number = 0
            
            # Debug file creation
            if hasattr(self, 'DEBUG') and self.DEBUG:
                filename = f"MrAttractor/LoopPhase3_{cnt_loop+1}/test/test_DI_CI_EI_{cnt_loop+1}-r-00000"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                self.logSingle = open(filename, 'w')
            
            for edge_key, p_edge_value in p_edges.items():
                parts = edge_key.split()
                i_begin = int(parts[0])
                i_end = int(parts[1])
                
                _dd_di = 0
                _dd_ei = 0
                _dd_ci = 0
                
                if 0 < p_edge_value.aDistance[self.m_iCurrentStep] < 1:
                    # Debug check for specific edge
                    if i_begin == 2604 and i_end == 2601:
                        pass  # Debug breakpoint equivalent
                    
                    d_di = self.compute_di(i_begin, i_end, p_edge_value)
                    d_ci = self.compute_ci(i_begin, i_end, p_edge_value)
                    d_ei = self.compute_ei(i_begin, i_end, p_edge_value)
                    
                    delta = d_di + d_ci + d_ei
                    _dd_di, _dd_ei, _dd_ci = d_di, d_ei, d_ci
                    
                    if abs(delta) > Settings.PRECISE:
                        # Add delta to delta window of edge
                        if Settings.SLIDING_WINDOW_SIZE > 0:
                            delta = p_edge_value.add_new_delta_to_window(delta)
                        
                        new_distance = self.m_cGraph.distance(i_begin, i_end, self.m_iCurrentStep) + delta
                        
                        if new_distance > 1 - Settings.PRECISE:
                            new_distance = 1
                        elif new_distance < Settings.PRECISE:
                            new_distance = 0
                        
                        self.m_cGraph.update_edge(i_begin, i_end, new_distance, i_next_step)
                        self.m_cGraph.add_vertex_weight(i_begin, new_distance, i_next_step)
                        self.m_cGraph.add_vertex_weight(i_end, new_distance, i_next_step)
                        b_continue = True
                else:
                    p_edge_value.aDistance[i_next_step] = p_edge_value.aDistance[self.m_iCurrentStep]
                    new_distance = p_edge_value.aDistance[self.m_iCurrentStep]
                    self.m_cGraph.add_vertex_weight(i_begin, new_distance, i_next_step)
                    self.m_cGraph.add_vertex_weight(i_end, new_distance, i_next_step)
                    i_converge_number += 1
                
                # Debug logging
                if hasattr(self, 'DEBUG') and self.DEBUG and self.logSingle:
                    test_str = f"{edge_key}, oldDis: {self.m_cGraph.distance(i_begin, i_end, self.m_iCurrentStep):.8f}, newDis: {self.m_cGraph.distance(i_begin, i_end, i_next_step):.8f}, DI: {_dd_di:.8f}, EI: {_dd_ei:.8f}, CI: {_dd_ci:.8f}"
                    self.logSingle.write(test_str + "\n")
            
            if hasattr(self, 'DEBUG') and self.DEBUG and self.logSingle:
                self.logSingle.close()
            
            toc = time.time() * 1000  # Convert to milliseconds
            cnt_loop += 1
            loop_single += 1
            
            self.logMaster.write(f"Current Iteration of single machine: {cnt_loop} Running Time: {(toc-tic)/1000.0}\n")
            self.logMaster.flush()
            
            self.m_cGraph.clear_vertex_weight(self.m_iCurrentStep)
            self.m_dictVirtualEdgeTempResult.clear()
            self.m_dictInteration[i] = i_converge_number
            self.m_iCurrentStep = Helper.update_step(self.m_iCurrentStep)
        
        # Update cnt loop
        self.logMaster.write(f"#Loops of single machine: {loop_single-1} #Loops single+MR is: {cnt_loop}\n")
        
        # Write output
        with open(self.outputfile, 'w') as writer:
            for edge_key, p_edge_value in p_edges.items():
                parts = edge_key.split()
                i_begin = int(parts[0])
                i_end = int(parts[1])
                s = f"{i_begin} {i_end} {p_edge_value.aDistance[self.m_iCurrentStep]:.8f} G"
                writer.write(s + "\n")

    def set_union(self, left: Set[int], right: Set[int]) -> Set[int]:
        """Set union operation."""
        return left.union(right)

    def set_difference(self, left: Set[int], right: Set[int]) -> Set[int]:
        """Set difference operation."""
        return left.difference(right)

    def set_intersection(self, left: Set[int], right: Set[int]) -> Set[int]:
        """Set intersection operation."""
        return left.intersection(right)

    def compute_di(self, i_begin: int, i_end: int, p_edge_value: EdgeValue) -> float:
        """Compute Direct Interaction."""
        return -math.sin(1 - p_edge_value.aDistance[self.m_iCurrentStep]) * (
            1 / (len(self.m_cGraph.get_vertex_neighbours(i_begin)) - 1) +
            1 / (len(self.m_cGraph.get_vertex_neighbours(i_end)) - 1)
        )

    def compute_ci(self, i_begin: int, i_end: int, p_edge_value: EdgeValue) -> float:
        """Compute Common Interaction."""
        if i_begin == 2604 and i_end == 2601:
            pass  # Debug breakpoint equivalent
        
        d_ci = 0
        
        for i_shared_vertex in p_edge_value.pCommonNeighbours:
            # Avoid re-computation
            if i_begin == i_shared_vertex or i_end == i_shared_vertex:
                continue
            
            d_begin = self.m_cGraph.distance(i_begin, i_shared_vertex, self.m_iCurrentStep)
            d_end = self.m_cGraph.distance(i_end, i_shared_vertex, self.m_iCurrentStep)
            
            d_ci += (math.sin(1 - d_begin) * (1 - d_end) / (len(self.m_cGraph.get_vertex_neighbours(i_begin)) - 1) +
                     math.sin(1 - d_end) * (1 - d_begin) / (len(self.m_cGraph.get_vertex_neighbours(i_end)) - 1))
        
        return -d_ci

    def compute_ei(self, i_begin: int, i_end: int, p_edge_value: EdgeValue) -> float:
        """Compute Exclusive Interaction."""
        d_ei = 0
        
        d_ei += (self.compute_partial_ei(i_begin, i_end, p_edge_value.pExclusiveNeighbours[Settings.BEGIN_POINT]) +
                 self.compute_partial_ei(i_end, i_begin, p_edge_value.pExclusiveNeighbours[Settings.END_POINT]))
        
        return -d_ei

    def compute_partial_ei(self, i_target: int, i_target_neighbour: int, target_en: Set[int]) -> float:
        """Compute partial Exclusive Interaction."""
        d_distance = 0
        
        for vertex in target_en:
            d_distance += (math.sin(1 - self.m_cGraph.distance(vertex, i_target, self.m_iCurrentStep)) *
                          self.compute_influence(i_target_neighbour, vertex, i_target) /
                          (len(self.m_cGraph.get_vertex_neighbours(i_target)) - 1))
        
        return d_distance

    def compute_influence(self, i_target_neighbour: int, i_en_vertex: int, i_target: int) -> float:
        """Compute influence between vertices."""
        d_distance = 1 - self.compute_virtual_distance(i_target_neighbour, i_en_vertex, i_target)
        
        if d_distance >= Settings.lambda_val:
            return d_distance
        
        return d_distance - Settings.lambda_val

    def compute_exclusive_neighbour(self, i_begin: int, i_end: int, p_edge_value: EdgeValue) -> None:
        """Compute exclusive neighbors."""
        p_edge_value.pExclusiveNeighbours[Settings.BEGIN_POINT] = self.set_difference(
            self.m_cGraph.get_vertex_neighbours(i_begin), p_edge_value.pCommonNeighbours
        )
        p_edge_value.pExclusiveNeighbours[Settings.END_POINT] = self.set_difference(
            self.m_cGraph.get_vertex_neighbours(i_end), p_edge_value.pCommonNeighbours
        )

    def compute_common_neighbour(self, i_begin: int, i_end: int, p_edge_value: EdgeValue) -> None:
        """Compute common neighbors."""
        p_edge_value.pCommonNeighbours = self.set_intersection(
            self.m_cGraph.get_vertex_neighbours(i_begin),
            self.m_cGraph.get_vertex_neighbours(i_end)
        )

    def compute_virtual_distance(self, i_begin: int, i_end: int, i_target: int) -> float:
        """Compute virtual distance between vertices."""
        i_temp_begin = i_begin
        i_temp_end = i_end
        
        edge_key = Graph.refine_edge_key(i_temp_begin, i_temp_end)
        
        if edge_key in self.m_dictVirtualEdgeTempResult:
            return self.m_dictVirtualEdgeTempResult[edge_key]
        
        d_numerator = 0
        p_begin_neighbours = self.m_cGraph.get_vertex_neighbours(i_begin)
        p_end_neighbours = self.m_cGraph.get_vertex_neighbours(i_end)
        
        set_common_neighbours = self.set_intersection(p_begin_neighbours, p_end_neighbours)
        
        for vertex in set_common_neighbours:
            d_begin = self.m_cGraph.distance(i_begin, vertex, self.m_iCurrentStep)
            d_end = self.m_cGraph.distance(i_end, vertex, self.m_iCurrentStep)
            d_numerator += (1 - d_begin) + (1 - d_end)
        
        d_denominator = (self.m_cGraph.get_vertex_weight_sum(i_begin, self.m_iCurrentStep) +
                        self.m_cGraph.get_vertex_weight_sum(i_end, self.m_iCurrentStep))
        
        d_distance = 1 - d_numerator / d_denominator
        
        if len(self.m_dictVirtualEdgeTempResult) < Settings.LIMIT_SIZE_DICT_VIRTUAL_EDGES:
            self.m_dictVirtualEdgeTempResult[edge_key] = d_distance
        
        return d_distance

    def execute(self, str_filename: str, merged_sliding_windows_file: str) -> None:
        """
        Run Attractor single machine.
        
        Args:
            str_filename: Input file path
            merged_sliding_windows_file: Merged sliding windows file path
        """
        self.setup_graph(str_filename)
        self.initialize_graph()
        self.update_sliding_window(merged_sliding_windows_file)
        self.dynamic_interaction()