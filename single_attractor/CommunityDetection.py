import time
import math
from typing import Set
from libs.Edge import Edge
from libs.Graph import Graph
from attractor.GraphUtils import GraphUtils
from pyspark.sql.types import Row


class CommunityDetection:
    @staticmethod
    def compute_next_step(index):
        return 1 if index == 0 else 0

    @staticmethod
    def compute_di(graph, v1, v2, edge, step) -> float:
        return -math.sin(1 - edge.a_distance[step]) * (
            1 / (len(graph.get_vertex_neighbours(v1)) - 1)
            + 1 / (len(graph.get_vertex_neighbours(v2)) - 1)
        )

    @staticmethod
    def compute_ci(graph: Graph, v1, v2, edge: Edge, step) -> float:
        d_ci = 0

        for shared in edge.common_n:
            if v1 == shared or v2 == shared:
                continue

            distance_1 = graph.distance(v1, shared, step)
            distance_2 = graph.distance(v2, shared, step)

            d_ci += math.sin(1 - distance_1) * (1 - distance_2) / (
                len(graph.get_vertex_neighbours(v1)) - 1
            ) + math.sin(1 - distance_2) * (1 - distance_1) / (
                len(graph.get_vertex_neighbours(v2)) - 1
            )

        return -d_ci

    @staticmethod
    def compute_ei(
        graph: Graph, v1, v2, edge: Edge, step: int, temp_result: dict
    ) -> float:
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

        if edge_key in temp_result.keys():
            return temp_result[edge_key]

        d_numerator = 0
        v1_neigh, v2_neigh = (
            graph.get_vertex_neighbours(v1),
            graph.get_vertex_neighbours(v2),
        )

        common_n = list(set(v1_neigh).intersection(set(v2_neigh)))

        for vertex in common_n:
            d_numerator += (1 - graph.distance(v1, vertex, step)) + (
                1 - graph.distance(v2, vertex, step)
            )

        d_denominator = graph.get_vertex_weight_sum(
            v1, step
        ) + graph.get_vertex_weight_sum(v2, step)

        distance = 1 - d_numerator / d_denominator
        temp_result[edge_key] = distance
        return distance

    @staticmethod
    def dynamic_interaction(graph: Graph, win_size: int):
        PRECISE = 0.0000001

        loop_counter = 0
        current_step = 0
        dictVirtEdges = dict()

        b_continue = True

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
                            delta = edge_value.add_new_delta_2_window(delta, win_size)

                        new_distance = (
                            graph.distance(v_start, v_end, current_step) + delta
                        )
                        if new_distance > 1 - PRECISE:
                            new_distance = 1
                        elif new_distance < PRECISE:
                            new_distance = 0

                        graph.update_edge(v_start, v_end, new_distance, next_step)
                        graph.add_vertex_weight(v_start, new_distance, next_step)
                        graph.add_vertex_weight(v_end, new_distance, next_step)
                        b_continue = True
                else:
                    edge_value.a_distance[next_step] = edge_value.a_distance[
                        current_step
                    ]
                    new_distance = edge_value.a_distance[current_step]
                    graph.add_vertex_weight(v_start, new_distance, next_step)
                    graph.add_vertex_weight(v_end, new_distance, next_step)
                    edges_converged_number += 1

            loop_counter += 1

            graph.clear_vertex_weight(current_step)
            dictVirtEdges = dict()
            current_step = CommunityDetection.compute_next_step(current_step)

        p_edges = graph.get_all_edges()
        res = []
        for key, edge in p_edges.items():
            vertex_start, vertex_end = Graph.from_key_to_vertex(key)
            (
                res.append(
                    (
                        Graph.refine_edge_key(vertex_start, vertex_end),
                        ("G", vertex_end, edge.a_distance[current_step], [], -1, -1),
                    )
                ),
            )

        return res

    @staticmethod
    def execute(reduced_edges, window_size, current_loop):
        graph_utils = GraphUtils()
        initialized_graph = graph_utils.init_jaccard_from_rdd(
            reduced_edges, current_loop
        )
        return CommunityDetection.dynamic_interaction(initialized_graph, window_size)
