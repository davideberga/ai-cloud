from pyspark.sql.types import Row
import numpy as np
from .MRStarGraphWithPrePartitions import MRStarGraphWithPrePartitions


class MRUpdateEdges:
    @staticmethod
    def mapReduce(
        df_graph_jaccard,
        rdd_dynamic_interactions,
        tau_,
        window_size_,
        iterations_counter_,
    ) -> int:
        tau = tau_
        window_size = window_size_
        iterations_counter = iterations_counter_

        # union = df_graph_jaccard.union(rdd_dynamic_interactions).groupByKey()

        zero_value = (0, 0, None)  # (current_weight, total_update)
        union = rdd_dynamic_interactions.aggregateByKey(
            zero_value, MRUpdateEdges.seq_op, MRUpdateEdges.comb_op
        )
        # combined = union.map(MRUpdateEdges.combiner)
        output_rdd = union.map(
            lambda x: MRUpdateEdges.reduce_function(
                x, tau, window_size, iterations_counter
            )
        )

        return output_rdd

    @staticmethod
    def rr(acc, rdd_edge):
        current_weight, total_update = acc
        current_weight = rdd_edge[3]
        total_update += rdd_edge[2]

        return (current_weight, total_update)

    @staticmethod
    def seq_op(acc, rdd_edge):
        current_weight, total_update, sliding = acc
        current_weight = rdd_edge[3]
        total_update += rdd_edge[2]
        return (current_weight, total_update, sliding)

    @staticmethod
    def comb_op(a, b):
        # merge two accumulators
        current_weight = b[0] if b[0] != 0 else a[0]
        total_update = a[1] + b[1]
        sliding = a[2] if a[2] is not None else b[2]
        return (current_weight, total_update, sliding)

    @staticmethod
    def map_function(rdd_edge):
        center = rdd_edge[0]
        sliding = []
        if len(rdd_edge[1]) > 3:
            edge_type, target, weight, sliding = rdd_edge[1]
        else:
            edge_type, target, weight = rdd_edge[1]
        return (f"{center}-{target}", (edge_type, weight, sliding))

    @staticmethod
    def combiner(edge_with_updates):
        edge, ups = edge_with_updates
        weight = 0
        containInteractions = False

        result = []
        sliding_data = []

        for update in ups:
            if update[0] == "I":
                weight += update[2]
                containInteractions = True
            else:
                sliding_data = update[3]
                result.append(update)

        if containInteractions:
            result.append(("I", weight))

        result.insert(0, sliding_data)
        return (edge, result)

    @staticmethod
    def reduce_function(union_data, tau, window_size, iterations_counter):
        edge_raw, updates = union_data

        center, target = edge_raw.split("-")
        center, target = int(center), int(target)

        edge = Row(center=center, target=target)
        usingSlidingWindow = window_size > 0

        delta_t =  updates[1]
        dis_uv = updates[0]

        key = f"{edge.center}-{edge.target}"
        sliding_data = (key, np.zeros((window_size), dtype=bool))

        

        # if usingSlidingWindow and iterations_counter > 0:
        #     deltaWindow = updates[2]
        #     # print(deltaWindow)
        # else:
        deltaWindow = np.zeros((32), dtype=bool)

        if dis_uv < 0:
            return Row(), sliding_data

        if dis_uv < 1 and dis_uv > 0:
            delta_t, sliding_data = MRUpdateEdges.updateDeltaWindow(
                edge, delta_t, iterations_counter, window_size, deltaWindow, tau
            )

        d_t_1 = dis_uv + delta_t
        if d_t_1 > 1:
            d_t_1 = 1.0
        if d_t_1 < 0:
            d_t_1 = 0.0

        return (edge_raw, ("G", edge.target, d_t_1, sliding_data))

    @staticmethod
    def updateDeltaWindow(
        edge, delta, iterations_counter, window_size, deltaWindow, tau
    ):
        index = iterations_counter % window_size
        deltaWindow[index] = bool(delta >= 0)

        returnedDelta = delta
        if iterations_counter >= (window_size - 1):
            if deltaWindow[index]:
                sameSize = np.sum(deltaWindow)
                if sameSize > (tau * window_size):
                    returnedDelta = 2
            else:
                sameSize = window_size - np.sum(deltaWindow)
                if sameSize > tau * window_size:
                    returnedDelta = -2

        return returnedDelta, deltaWindow
