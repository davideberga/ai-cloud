import time
import math
from typing import Set
from libs.Graph import Graph
from attractor.GraphUtils import GraphUtils
from args_parser import parse_arguments

class CommunityDetection:

    step = 0
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
        
    def update_sliding_window(graph, previousSlidingWindow) -> None:
       
        if not previousSlidingWindow:
            return
        
        # key: str'u-v', values: np.array[True, False, ...]
        for edge, window in previousSlidingWindow.items():
            key = edge.replace('-', ' ')

            if key in graph.get_all_edges().keys():
                graph.get_all_edges()[key].sliding_window = window
                #print(f"[Sliding Window Updated] Edge: {key}, Window: {window}")
            else:
                print(f"[Edge not found]")

    
    def dynamic_interaction(graph):
        """
        Dynamic Interaction main loop.
        """
        b_continue = True
        cnt_loop = 0
        
        while b_continue:
            b_continue = False
            print(f"Single Machine Current Loop: {cnt_loop + 1}")
            
            tic = time.time() * 1000  # Convert to milliseconds
            i_converge_number = 0

            # For each edge we analize if the distance is significant
            for key in graph.get_all_edges().keys():
                if 0 < graph.get_all_edges()[key].aDistance[CommunityDetection.step] < 1:
                    i_begin, i_end = Graph.from_key_to_vertex(key)
                    edge_value = graph.get_all_edges()[key]
                    print(f"Processing edge: {key}, aDistance: {edge_value.aDistance[CommunityDetection.step]}")

                    d_di = CommunityDetection.compute_di(i_begin, i_end, edge_value)
                    d_ci = CommunityDetection.compute_ci(i_begin, i_end, edge_value)
                    d_ei = CommunityDetection.compute_ei(i_begin, i_end, edge_value)

                    delta = d_di + d_ci + d_ei

                    if delta > CommunityDetection.precise or delta < -CommunityDetection.precise:
                        # Add delta to delta window of edge
                        if CommunityDetection.args.window_size > 0:
                            delta = edge_value.add_new_delta_2_window(delta)

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
    def execute(reduced_edges, previousSlidingWindow, num_vertices):
        #print("Executing Community Detection...", reduced_edges)
        #print("previousSlidingWindow:", previousSlidingWindow.value)

        graph_utils = GraphUtils()
        initialized_graph = graph_utils.init_jaccard_from_rdd(reduced_edges)
        #print(f"get edges: {initialized_graph.get_all_edges()}")
        CommunityDetection.update_sliding_window(initialized_graph, previousSlidingWindow.value)

        CommunityDetection.dynamic_interaction(initialized_graph)