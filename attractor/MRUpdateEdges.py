from pyspark.sql.types import Row
import numpy as np


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

        rdd_dynamic_interactions = df_graph_jaccard.union(rdd_dynamic_interactions)
        union_output = rdd_dynamic_interactions.map(MRUpdateEdges.map_function)
        union_output = union_output.groupByKey()
        union_output = union_output.flatMap(MRUpdateEdges.combiner)
        union_output = union_output.groupByKey()
        output_rdd = union_output.map(
            lambda x: MRUpdateEdges.reduce_function(
                x, tau, window_size, iterations_counter, previousSlidingWindow
            )
        )
        rows_rdd = output_rdd.map(lambda x: x[0])
        sliding_data_rdd = output_rdd.map(lambda x: x[1])

        return rows_rdd, sliding_data_rdd

    @staticmethod
    def map_function(rdd_edge):
        type = getattr(rdd_edge, "type", None)
        center = getattr(rdd_edge, "center", None)
        target = getattr(rdd_edge, "target", None)
        weight = getattr(rdd_edge, "weight", None)

        edge = Row(center=center, target=target)
        spec = Row(type=type, weight=weight)

        return (edge, spec)

    @staticmethod
    def combiner(edge_with_updates):
        weight = 0
        containInteractions = False
        result = []

        for update in edge_with_updates[1]:
            if update.type == "I":
                weight += update.weight
                containInteractions = True
            else:
                result.append((edge_with_updates[0], update))

        if containInteractions:
            spec = Row(type="I", weight=weight)
            result.append((edge_with_updates[0], spec))

        return result

    @staticmethod
    def reduce_function(union_data, tau, window_size, iterations_counter, previousSlidingWindow):
        # union_data is a list of Row (edge, spec)
        edge, updates = union_data

        if window_size > 0:
            usingSlidingWindow = True
        else:
            usingSlidingWindow = False

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
        
        if dis_uv < 0: return (), ()
        
        if dis_uv < 1 and dis_uv > 0 and usingSlidingWindow: 
            delta_t, sliding_data = MRUpdateEdges.updateDeltaWindow(edge, delta_t, iterations_counter, window_size, deltaWindow, tau)

        d_t_1 = dis_uv + delta_t
        if d_t_1 > 1:
            d_t_1 = 1.0
        if d_t_1 < 0:
            d_t_1 = 0.0
                        
        return Row(type="G", center=edge.center, target=edge.target, weight=d_t_1), sliding_data


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