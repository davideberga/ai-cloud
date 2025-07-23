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
    ) -> int:
        tau = tau_
        window_size = window_size_
        iterations_counter = iterations_counter_

        rdd_dynamic_interactions = df_graph_jaccard.union(rdd_dynamic_interactions)
        union_output = rdd_dynamic_interactions.map(MRUpdateEdges.map_function)
        union_output = union_output.groupByKey()
        union_output = union_output.flatMap(MRUpdateEdges.combiner)
        union_output = union_output.groupByKey()
        union_output = union_output.map(
            lambda x: MRUpdateEdges.reduce_function(
                x, tau, window_size, iterations_counter
            )
        )

        return union_output

    @staticmethod
    def map_function(rdd_edge):
        center = rdd_edge[0]
        rdd_edge = rdd_edge[1][0]
        edge_type, target, weight = rdd_edge["type"], rdd_edge["target"], rdd_edge["weight"]
        type_ = edge_type

        edge = Row(center=center, target=target)

        if edge_type == "L":
            spec = Row()
        else:
            spec = Row(type=type_, weight=weight)

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
    def reduce_function(union_data, tau, window_size, interations_counter):
        # union_data is a list of Row (edge, spec)
        edge, updates = union_data

        delta_t = 0
        dis_uv = -1

        existDeltat = False
        
        deltaWindow = np.zeros((32), dtype=bool)

        for update in updates:
            
            type = update.type
            weight = update.weight
            
            if type == "G":
                dis_uv = weight
            elif type == "I":
                delta_t += weight
                existDeltat = True
            elif type == "L":
                # TODO nBits
                # TODO delta
                raise UnicodeDecodeError("Missing nBits, delta window")
                pass
        
        if dis_uv < 0: return Row()
        
        if dis_uv < 1 and dis_uv > 0: 
            delta_t = MRUpdateEdges.updateDeltaWindow(edge, delta_t, interations_counter, window_size, deltaWindow, tau)

        d_t_1 = dis_uv + delta_t
        if d_t_1 > 1:
            d_t_1 = 1.0
        if d_t_1 < 0:
            d_t_1 = 0.0
            
        return Row(type="G", center=edge.center, target=edge.target, weight=d_t_1)


    @staticmethod
    def updateDeltaWindow(edge, delta, interations_counter, window_size, deltaWindow, tau):
        index = interations_counter % window_size
        deltaWindow[index] = bool(delta >= 0)
        
        returnedDelta = delta
        if interations_counter >= (window_size -1):
            if deltaWindow[index]:
                sameSize = np.sum(deltaWindow)
                if sameSize > tau * window_size: 
                    returnedDelta = 2
            else:
                sameSize = window_size - np.sum(deltaWindow)
                if sameSize > tau * window_size: 
                    returnedDelta = -2
        else:
            pass
        
        return returnedDelta
            
    
            
        
