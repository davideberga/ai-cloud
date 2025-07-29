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
        previousSlidingWindow
    ) -> int:
        tau = tau_
        window_size = window_size_
        iterations_counter = iterations_counter_
        
      
        union = df_graph_jaccard.union(rdd_dynamic_interactions)
        union = union.map(MRUpdateEdges.map_function)
        reduced = union.reduceByKey(MRStarGraphWithPrePartitions.aggregate_comb)
        combined = reduced.map(MRUpdateEdges.combiner)
        output_rdd = combined.map(
            lambda x: MRUpdateEdges.reduce_function(
                x, tau, window_size, iterations_counter, previousSlidingWindow
            )
        )
        rows_rdd = output_rdd.map(lambda x: x[0])
        sliding_data_rdd = output_rdd.map(lambda x: x[1])

        return rows_rdd, sliding_data_rdd

    @staticmethod
    def map_function(rdd_edge):
        center = rdd_edge[0]
        rdd_edge = rdd_edge[1][0]
        edge_type, target, weight = rdd_edge["type"], rdd_edge["target"], rdd_edge["weight"]
        type_ = edge_type
        
        spec = Row(type=type_, weight=weight)
        return (f"{center}-{target}", [spec])

    @staticmethod
    def combiner(edge_with_updates):
        edge, ups = edge_with_updates
        weight = 0
        containInteractions = False
        result = []

        for update in ups:
            if update["type"] == "I":
                weight += update["weight"]
                containInteractions = True
            else:
                result.append(update)

        if containInteractions:
            spec = Row(type="I", weight=weight)
            result.append(spec)

        return (edge, result)

    @staticmethod
    def reduce_function(union_data, tau, window_size, iterations_counter, previousSlidingWindow):

        edge, updates = union_data
        center, target = edge.split("-")
        center, target = int(center), int(target)
        
        edge = Row(center=center, target=target)
        usingSlidingWindow = window_size > 0

        delta_t = 0
        dis_uv = -1

        key = f"{edge.center}-{edge.target}"
        sliding_data = (key, np.zeros((window_size), dtype=bool))

        deltaWindow = np.zeros((32), dtype=bool)

        if usingSlidingWindow and previousSlidingWindow is not None:
            deltaWindow = previousSlidingWindow.value[key]

        for update in updates:
            
            type = update.type
            weight = update.weight
            
            if type == "G":
                dis_uv = weight
            elif type == "I":
                delta_t += weight                     
        
        if dis_uv < 0: return Row(), sliding_data
        
        if dis_uv < 1 and dis_uv > 0: 
            delta_t, sliding_data = MRUpdateEdges.updateDeltaWindow(edge, delta_t, iterations_counter, window_size, deltaWindow, tau)

        d_t_1 = dis_uv + delta_t
        if d_t_1 > 1:
            d_t_1 = 1.0
        if d_t_1 < 0:
            d_t_1 = 0.0
            
        return (edge.center, [{'type': 'G', 'target': edge.target, 'weight': d_t_1}]), sliding_data


    @staticmethod
    def updateDeltaWindow(edge, delta, iterations_counter, window_size, deltaWindow, tau):
        index = iterations_counter % window_size
        deltaWindow[index] = bool(delta >= 0)

        returnedDelta = delta
        if iterations_counter >= (window_size -1):    
            if deltaWindow[index]:
                sameSize = np.sum(deltaWindow)
                if sameSize > (tau * window_size): 
                    returnedDelta = 2
            else:
                sameSize = window_size - np.sum(deltaWindow)
                if sameSize > tau * window_size: 
                    returnedDelta = -2
    
        return returnedDelta, (f"{edge.center}-{edge.target}", deltaWindow)