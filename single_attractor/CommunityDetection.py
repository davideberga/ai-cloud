import os
import math
from typing import Set
from libs.Graph import Graph
from attractor.GraphUtils import GraphUtils
from args_parser import parse_arguments

class CommunityDetection:

    step = 0
    graph = Graph()
    current_loops = 0
    begin_point = 0
    end_point = 1
    args = parse_arguments()
    dictVirtualEdgeTempResult = {}
    dictInteration = {}
    limit_size_dict_virtual_edges = -1
    precise = 0.0000001
    n_edges_reduce_graph = 0
    n_vertices_reduce_graph = 0

    def setup_graph(self, reduced_edges): #str_filename
        
        for row in reduced_edges:
            i_begin = row.center
            i_end = row.target
            d_weight = row.weight
            self.graph.add_edge(i_begin, i_end, d_weight)
            self.n_edges_reduce_graph += 1

        self.n_vertices_reduce_graph = len(self.graph.m_dict_vertices)

    def initialize_graph(self, graph: Graph) -> Graph:
        """
        Pre-compute the common neighbors and exclusive neighbors of every
        non-converged edges. Pre-compute sumWeight of every node in G(V,E)
        """
        
        p_edges = graph.get_all_edges()
        cnt_check_sum_weight = 0
        
        for edge_key, edge_info in p_edges.items():
            
            vertex_start, vertex_end = Graph.from_key_to_vertex(edge_key)
            
            # Get neighbors of both vertices
            star_u = graph.m_dict_vertices[vertex_start].pNeighbours
            star_v = graph.m_dict_vertices[vertex_end].pNeighbours
            
            # Count common neighbors using two-pointer technique
            i, j = 0, 0
            m, n = len(star_u), len(star_v)
            
            no_common_neighbor = 0
            
            while i < m and j < n:
                a = star_u[i]
                b = star_v[j]
    
                if a == b:
                    no_common_neighbor += 1
                    i += 1
                    j += 1
                elif a > b:
                    j += 1
                else:  # a < b
                    i += 1
            
            # Calculate Jaccard distance
            deg_u = len(star_u) - 1  # Subtract 1 to exclude self
            deg_v = len(star_v) - 1  # Subtract 1 to exclude self
            c = no_common_neighbor
            
            numerator = float(c)  # Common neighbors
            denominator = float(deg_u + deg_v + 2 - c)
            
            # Jaccard distance = 1 - (intersection / union)
            dis = 1.0 - numerator / denominator if denominator != 0 else 1.0
            
            edge_info.weight = dis
            
            graph.update_edge(vertex_start, vertex_end, dis, self.step)
            graph.add_vertex_weight(vertex_start, dis, self.step)
            graph.add_vertex_weight(vertex_end, dis, self.step)
            
            cnt_check_sum_weight += 1
        

    def update_sliding_window(self, sliding_data) -> None:
        """
        Updating sliding window from MR version.
        
        Args:
            merged_sliding_windows_file: Path to merged sliding windows file
        """
        if not sliding_data:
            return
        
        no_loops_mr = min(self.current_loops + 1, self.args.window_size)

        for sliding in sliding_data:
            u = sliding.center
            v = sliding.target
            edge_key = Graph.refine_edge_key(u, v)
        
            if edge_key in self.graph.m_dictEdges:
                vl = self.graph.m_dictEdges[edge_key]
                status = []
                for j in range(3, len(sliding)):
                    s = int(sliding[j])
                    assert s == 0 or s == 1
                    status.append(s)
                vl.set_sliding_window(no_loops_mr, status)
        
    @staticmethod
    def dynamic_interaction(self):
        """
        Dynamic Interaction main loop.
        """
        b_continue = True
        p_edges = self.graph.get_all_edges()

        i = 0
        cnt_loop = self.current_loops
        loop_single = 0
        
        while b_continue:
            b_continue = False
            i_next_step = self.step + 1
            print(f"Single Machine Current Loop: {cnt_loop + 1}")
            
            import time
            tic = time.time() * 1000  # Convert to milliseconds
            i_converge_number = 0
            
            for edge_key, p_edge_value in p_edges.items():
                i_begin = edge_key.center
                i_end = edge_key.target
                
                _dd_di = 0
                _dd_ei = 0
                _dd_ci = 0
                
                if 0 < p_edge_value.weight[self.step] < 1:
                    
                    d_di = CommunityDetection.compute_di(i_begin, i_end, p_edge_value)
                    d_ci = CommunityDetection.compute_ci(i_begin, i_end, p_edge_value)
                    d_ei = CommunityDetection.compute_ei(i_begin, i_end, p_edge_value)
                    
                    delta = d_di + d_ci + d_ei
                    _dd_di, _dd_ei, _dd_ci = d_di, d_ei, d_ci
                    
                    if abs(delta) > self.precise:
                        # Add delta to delta window of edge
                        if self.args.window_size > 0:
                            delta = p_edge_value.add_new_delta_to_window(delta)

                        new_distance = self.graph.weight(i_begin, i_end, self.step) + delta

                        if new_distance > 1 - self.precise:
                            new_distance = 1
                        elif new_distance < self.precise:
                            new_distance = 0

                        self.graph.update_edge(i_begin, i_end, new_distance, i_next_step)
                        self.graph.add_vertex_weight(i_begin, new_distance, i_next_step)
                        self.graph.add_vertex_weight(i_end, new_distance, i_next_step)
                        b_continue = True
                else:
                    p_edge_value.weight[i_next_step] = p_edge_value.weight[self.step]
                    new_distance = p_edge_value.weight[self.step]
                    self.graph.add_vertex_weight(i_begin, new_distance, i_next_step)
                    self.graph.add_vertex_weight(i_end, new_distance, i_next_step)
                    i_converge_number += 1
                
               
            toc = time.time() * 1000  # Convert to milliseconds
            cnt_loop += 1
            loop_single += 1
            print(f"Single Machine Loop {cnt_loop} Time: {(toc - tic) / 1000:.3f} seconds")

            self.graph.clear_vertex_weight(self.step)
            self.dictVirtualEdgeTempResult.clear()
            self.dictInteration[i] = i_converge_number
            self.step = i_next_step

        # Qui c'è da scrivere l'output

    @staticmethod
    def set_union( left: Set[int], right: Set[int]) -> Set[int]:
        """Set union operation."""
        return left.union(right)

    @staticmethod
    def set_difference(left: Set[int], right: Set[int]) -> Set[int]:
        """Set difference operation."""
        return left.difference(right)
    
    @staticmethod
    def set_intersection(left, right):
        """Set intersection operation."""
        return list(set(left).intersection(set(right)))

    @staticmethod
    def compute_di(self, i_begin, i_end, p_edge_value) -> float:
        """Compute Direct Interaction."""
        return -math.sin(1 - p_edge_value.aDistance[self.step]) * (
            1 / (len(self.graph.get_vertex_neighbours(i_begin)) - 1) +
            1 / (len(self.graph.get_vertex_neighbours(i_end)) - 1)
        )

    @staticmethod
    def compute_ci(self, i_begin, i_end, p_edge_value) -> float:
        """Compute Common Interaction."""
        
        d_ci = 0
        
        for i_shared_vertex in p_edge_value.pCommonNeighbours:
            # Avoid re-computation
            if i_begin == i_shared_vertex or i_end == i_shared_vertex:
                continue

            d_begin = self.graph.weight(i_begin, i_shared_vertex, self.step)
            d_end = self.graph.weight(i_end, i_shared_vertex, self.step)
            
            d_ci += (math.sin(1 - d_begin) * (1 - d_end) / (len(self.graph.get_vertex_neighbours(i_begin)) - 1) +
                     math.sin(1 - d_end) * (1 - d_begin) / (len(self.graph.get_vertex_neighbours(i_end)) - 1))
        
        return -d_ci

    @staticmethod
    def compute_ei(self, i_begin: int, i_end: int, p_edge_value) -> float:
        """Compute Exclusive Interaction."""
        d_ei = 0

        d_ei += (CommunityDetection.compute_partial_ei(i_begin, i_end, p_edge_value.pExclusiveNeighbours[self.begin_point]) +
                 CommunityDetection.compute_partial_ei(i_end, i_begin, p_edge_value.pExclusiveNeighbours[self.end_point]))

        return -d_ei

    @staticmethod
    def compute_partial_ei(self, i_target: int, i_target_neighbour: int, target_en: Set[int]) -> float:
        """Compute partial Exclusive Interaction."""
        d_distance = 0
        
        for vertex in target_en:
            d_distance += (math.sin(1 - self.graph.weight(vertex, i_target, self.step)) *
                          CommunityDetection.compute_influence(i_target_neighbour, vertex, i_target) /
                          (len(self.graph.get_vertex_neighbours(i_target)) - 1))

        return d_distance

    @staticmethod
    def compute_influence(self, i_target_neighbour: int, i_en_vertex: int, i_target: int) -> float:
        """Compute influence between vertices."""
        d_distance = 1 - CommunityDetection.compute_virtual_distance(i_target_neighbour, i_en_vertex, i_target)

        if d_distance >= self.args.lambda_:
            return d_distance

        return d_distance - self.args.lambda_

    @staticmethod
    def compute_exclusive_neighbour(self,i_begin, i_end, p_edge_value) -> None:
        """Compute exclusive neighbors."""
        p_edge_value.pExclusiveNeighbours[self.begin_point] = CommunityDetection.set_difference(
            self.graph.get_vertex_neighbours(i_begin), p_edge_value.pCommonNeighbours
        )
        p_edge_value.pExclusiveNeighbours[self.end_point] = CommunityDetection.set_difference(
            self.graph.get_vertex_neighbours(i_end), p_edge_value.pCommonNeighbours
        )

    def compute_common_neighbour(self, i_begin, i_end, p_edge_value) -> None:
        """Compute common neighbors."""
        print(f"self.graph.m_dict_vertices[i_begin].pNeighbours: {self.graph.m_dict_vertices[i_begin].pNeighbours}")
        p_edge_value.pCommonNeighbours = CommunityDetection.set_intersection(
            self.graph.m_dict_vertices[i_begin].pNeighbours,
            self.graph.m_dict_vertices[i_end].pNeighbours
        )

    @staticmethod
    def compute_virtual_distance(self, i_begin: int, i_end: int, i_target: int) -> float:
        """Compute virtual distance between vertices."""
        i_temp_begin = i_begin
        i_temp_end = i_end
        
        edge_key = Graph.refine_edge_key(i_temp_begin, i_temp_end)

        if edge_key in self.dictVirtualEdgeTempResult:
            return self.dictVirtualEdgeTempResult[edge_key]

        d_numerator = 0
        p_begin_neighbours = self.graph.get_vertex_neighbours(i_begin)
        p_end_neighbours = self.graph.get_vertex_neighbours(i_end)

        set_common_neighbours = CommunityDetection.set_intersection(p_begin_neighbours, p_end_neighbours)
        
        for vertex in set_common_neighbours:
            d_begin = self.graph.weight(i_begin, vertex, self.step)
            d_end = self.graph.weight(i_end, vertex, self.step)
            d_numerator += (1 - d_begin) + (1 - d_end)

        d_denominator = (self.graph.get_vertex_weight_sum(i_begin, self.step) +
                        self.graph.get_vertex_weight_sum(i_end, self.step))

        d_distance = 1 - d_numerator / d_denominator

        if len(self.dictVirtualEdgeTempResult) < self.limit_size_dict_virtual_edges:
            self.dictVirtualEdgeTempResult[edge_key] = d_distance

        return d_distance

    @staticmethod
    def execute(reduced_edges, sliding_data):
        detector = CommunityDetection()
        detector.setup_graph(reduced_edges)
        detector.initialize_graph(detector.graph)
        detector.update_sliding_window(sliding_data)
        detector.dynamic_interaction()