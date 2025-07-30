import time
import math
from typing import Set
from libs.EdgeInfo import EdgeInfo
from libs.Graph import Graph
from attractor.GraphUtils import GraphUtils
from args_parser import parse_arguments


class CommunityDetection:
    @staticmethod
    def update_sliding_window(graph, previousSlidingWindow) -> None:
        if not previousSlidingWindow:
            return

        # key: str'u-v', values: np.array[True, False, ...]
        for edge, window in previousSlidingWindow.items():
            key = edge.replace("-", " ")

            if key in graph.get_all_edges().keys():
                graph.get_all_edges()[key].sliding_window = window
                # print(f"[Sliding Window Updated] Edge: {key}, Window: {window}")
            else:
                print(f"[Edge not found]")

    @staticmethod
    def compute_next_step(index):
        return 1 if index == 0 else 0

    @staticmethod
    def set_union(left: Set[int], right: Set[int]) -> Set[int]:
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
    def compute_di(graph, v1, v2, edge, step) -> float:
        return -math.sin(1 - edge.a_distance[step]) * (
            1 / (len(graph.get_vertex_neighbours(v1)) - 1)
            + 1 / (len(graph.get_vertex_neighbours(v2)) - 1)
        )

    @staticmethod
    def compute_ci(graph: Graph, v1, v2, edge: EdgeInfo, step) -> float:
        d_ci = 0

        for shared in edge.common_n:
            if v1 == shared or v2 == shared:
                continue

            distance_1 = graph.distance(v1, shared, step)
            distance_2 = graph.distance(v2, shared, step)

            d_ci += math.sin(1 - distance_1) * (1 - distance_2) / (
                len(graph.get_vertex_neighbours(v1)) - 1
            ) + math.sin(distance_2) * (1 - distance_1) / (
                len(graph.get_vertex_neighbours(v2)) - 1
            )

        return -d_ci

    @staticmethod
    def compute_ei(
        graph: Graph, v1, v2, edge: EdgeInfo, step: int, temp_result: dict
    ) -> float:
        """Compute Exclusive Interaction."""
        d_ei = 0

        d_ei += CommunityDetection.compute_partial_ei(
            graph, v1, v2, edge.exclusive_n[0], step, temp_result
        ) + CommunityDetection.compute_partial_ei(
            graph, v2, v1, edge.exclusive_n[1], step, temp_result
        )

        return -d_ei

    @staticmethod
    def compute_partial_ei(
        graph: Graph, v1: int, v2: int, exclN: Set[int], step: int, temp_result: dict
    ) -> float:
        d_distance = 0
        for vertex in exclN:
            d_distance += (
                math.sin(1 - graph.distance(vertex, v1, step))
                * CommunityDetection.compute_influence(
                    graph, v2, vertex, step, temp_result
                )
                / (len(graph.get_vertex_neighbours(v1)) - 1)
            )

        return d_distance

    @staticmethod
    def compute_influence(
        graph: Graph, v1: int, v2: int, step: int, temp_result: dict
    ) -> float:
        lambda_ = 0.5
        distance = 1 - CommunityDetection.compute_virtual_distance(
            graph, v1, v2, step, temp_result
        )
        return distance if distance >= lambda_ else distance - lambda_

    @staticmethod
    def compute_virtual_distance(
        graph: Graph, v1: int, v2: int, step: int, temp_result: dict
    ) -> float:
        edge_key = Graph.refine_edge_key(v1, v2)

        if edge_key in temp_result:
            return temp_result[edge_key]

        d_numerator = 0
        v1_neigh, v2_neigh = (
            graph.get_vertex_neighbours(v1),
            graph.get_vertex_neighbours(v2),
        )

        common_n = CommunityDetection.set_intersection(v1_neigh, v2_neigh)

        for vertex in common_n:
            d_numerator += (1 - graph.distance(v1, vertex, step)) + (
                1 - graph.weight(v2, vertex, step)
            )

        d_denominator = graph.get_vertex_weight_sum(
            v1, step
        ) + graph.get_vertex_weight_sum(v2, step)

        distance = 1 - d_numerator / d_denominator
        temp_result[edge_key] = distance
        return distance

    @staticmethod
    def compute_exclusive_neighbour(self, i_begin, i_end, p_edge_value) -> None:
        """Compute exclusive neighbors."""
        p_edge_value.pExclusiveNeighbours[self.begin_point] = (
            CommunityDetection.set_difference(
                self.graph.get_vertex_neighbours(i_begin),
                p_edge_value.pCommonNeighbours,
            )
        )
        p_edge_value.pExclusiveNeighbours[self.end_point] = (
            CommunityDetection.set_difference(
                self.graph.get_vertex_neighbours(i_end), p_edge_value.pCommonNeighbours
            )
        )

    def compute_common_neighbour(self, i_begin, i_end, p_edge_value) -> None:
        """Compute common neighbors."""
        print(
            f"self.graph.m_dict_vertices[i_begin].pNeighbours: {self.graph.m_dict_vertices[i_begin].pNeighbours}"
        )
        p_edge_value.pCommonNeighbours = CommunityDetection.set_intersection(
            self.graph.m_dict_vertices[i_begin].pNeighbours,
            self.graph.m_dict_vertices[i_end].pNeighbours,
        )

    @staticmethod
    def dynamic_interaction(graph: Graph, win_size: int):
        PRECISE = 0.0000001

        loop_counter = 0
        current_step = 0
        dictVirtEdges = dict()

        b_continue = True

        i = 0

        loop_single = 0

        while b_continue:
            b_continue = False

            next_step = CommunityDetection.compute_next_step(current_step)
            edges_converged_number = 0

            p_edges = graph.get_all_edges()
            for edge_key, edge_value in p_edges.items():
                v_start = edge_value.vertex_start
                v_end = edge_value.vertex_end

                di, ci, ei = 0, 0, 0

                if 0 < edge_value.a_distance[current_step] < 1:
                    di = CommunityDetection.compute_di(
                        graph, v_start, v_end, edge_value, current_step
                    )
                    ci = CommunityDetection.compute_ci(
                        graph, v_start, v_end, edge_value, current_step
                    )
                    ei = CommunityDetection.compute_ei(
                        graph, v_start, v_end, edge_value, current_step, dictVirtEdges
                    )

                    delta = di + ci + ei

                    if abs(delta) > PRECISE:
                        # Add delta to delta window of edge
                        if win_size > 0:
                            delta = edge_value.add_new_delta_to_window(delta)

                        new_distance = (
                            graph.weight(v_start, v_end, current_step) + delta
                        )
                        new_distance = int(new_distance > PRECISE)

                        graph.update_edge(v_start, v_end, new_distance, next_step)
                        graph.add_vertex_weight(v_start, new_distance, next_step)
                        graph.add_vertex_weight(v_end, new_distance, next_step)
                        b_continue = True
                else:
                    edge_value.weight[next_step] = edge_value.weight[current_step]
                    new_distance = edge_value.weight[current_step]
                    graph.add_vertex_weight(v_start, new_distance, next_step)
                    graph.add_vertex_weight(v_end, new_distance, next_step)
                    edges_converged_number += 1

            cnt_loop += 1
            loop_single += 1
            # print(f"Single Machine Loop {cnt_loop} Time: {(toc - tic) / 1000:.3f} seconds")

            graph.clear_vertex_weight(current_step)
            dictVirtEdges = dict()
            current_step = CommunityDetection.compute_next_step(current_step)

        # Qui c'è da scrivere l'output

    @staticmethod
    def execute(reduced_edges, window_size, previousSlidingWindow):
        graph_utils = GraphUtils()
        initialized_graph = graph_utils.init_jaccard_from_rdd(reduced_edges)

        CommunityDetection.update_sliding_window(
            initialized_graph, previousSlidingWindow.value
        )
        CommunityDetection.dynamic_interaction(initialized_graph, window_size)
