from pyspark.sql.types import Row
import numpy as np
from .MRStarGraphWithPrePartitions import MRStarGraphWithPrePartitions


class MRUpdateEdges:
    @staticmethod
    def mapReduce(
        rdd_dynamic_interactions,
        tau_,
        window_size_,
        iterations_counter_,
    ) -> int:
        tau = tau_
        window_size = window_size_
        iterations_counter = iterations_counter_

        # union = df_graph_jaccard.union(rdd_dynamic_interactions).groupByKey()

        zero_value = (0, 0, 0, 0, None)
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
    def seq_op(acc, rdd_edge):
        current_weight, total_update, deg_center, deg_target, sliding = acc
        current_weight = rdd_edge[3]
        total_update += rdd_edge[2]
        deg_center = rdd_edge[4]
        deg_target = rdd_edge[5]
        sliding = rdd_edge[6]
        return (current_weight, total_update, deg_center, deg_target, sliding)

    @staticmethod
    def comb_op(a, b):
        # merge two accumulators
        current_weight = b[0] if b[0] != 0 else a[0]
        total_update = a[1] + b[1]
        deg_center =  b[2] if b[2] != 0 else a[2]
        deg_target =  b[3] if b[3] != 0 else a[3]
        sliding = a[4] if a[4] is not None else b[4]
        return (current_weight, total_update, deg_center, deg_target, sliding)

    @staticmethod
    def reduce_function(union_data, tau, window_size, iterations_counter):
        edge_raw, updates = union_data
        
        # print(updates)

        center, target = edge_raw.split("-")
        center, target = int(center), int(target)

        edge = Row(center=center, target=target)
        usingSlidingWindow = window_size > 0

        delta_t =  updates[1]
        dis_uv = updates[0]

        key = f"{edge.center}-{edge.target}"
        deg_center = updates[2]
        deg_target =  updates[3]
        

        if usingSlidingWindow and iterations_counter > 0:
            deltaWindow = updates[4]        
        else:
            deltaWindow = np.zeros((window_size), dtype=bool)
            
        if dis_uv < 0: return (), ()

        if dis_uv < 1 and dis_uv > 0  and usingSlidingWindow:
            delta_t, sliding_data = MRUpdateEdges.updateDeltaWindow(edge, delta_t, iterations_counter, window_size, deltaWindow, tau)
        else:
            sliding_data = np.zeros(window_size, dtype=bool)
        
        d_t_1 = dis_uv + delta_t
        if d_t_1 > 1:
            d_t_1 = 1.0
        if d_t_1 < 0:
            d_t_1 = 0.0
            
        return (edge_raw, ("G", edge.target, d_t_1, sliding_data, deg_center, deg_target))

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
