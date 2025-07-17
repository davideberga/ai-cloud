import logging
import traceback
from typing import List, Tuple, Iterator
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

# class BitSet:
#     """Implementazione semplice di BitSet per simulare il comportamento Java"""

#     def __init__(self, size: int = 32):
#         self.size = size
#         self.bits = [False] * size

#     def set(self, index: int):
#         """Imposta il bit all'indice specificato"""
#         if 0 <= index < self.size:
#             self.bits[index] = True

#     def clear(self, index: int):
#         """Pulisce il bit all'indice specificato"""
#         if 0 <= index < self.size:
#             self.bits[index] = False

#     def get(self, index: int) -> bool:
#         """Ottiene il valore del bit all'indice specificato"""
#         if 0 <= index < self.size:
#             return self.bits[index]
#         return False

#     def cardinality(self) -> int:
#         """Restituisce il numero di bit impostati a True"""
#         return sum(self.bits)

# class LoopUpdateEdgesMapper:
#     """Mapper class per LoopUpdateEdges"""

#     def __init__(self):
#         self.logger = logging.getLogger(self.__class__.__name__)

#     def setup(self, context):
#         """Setup del mapper"""
#         pass

#     def map(self, key: SpecialEdgeTypeWritable, value, context) -> Iterator[Tuple[PairWritable, SpecialEdgeValueWritable]]:
#         """
#         Funzione map. Input: SpecialEdgeTypeWritable, Output: (PairWritable, SpecialEdgeValueWritable)
#         """
#         try:
#             # Verifica che il tipo sia valido
#             valid_types = [Settings.C_TYPE, Settings.D_TYPE, Settings.E_TYPE,
#                           Settings.EDGE_TYPE, Settings.INTERACTION_TYPE, Settings.SLIDING]
#             assert key.type in valid_types, f"Invalid key type: {key.type}"

#             edge = PairWritable()
#             edge.set(key.center, key.target)

#             spec = SpecialEdgeValueWritable()
#             if key.type == Settings.SLIDING:
#                 spec.init(key.type, key.weight, key.no_bits, key.sliding)
#             else:
#                 spec.init(key.type, key.weight, -1, None)

#             yield edge, spec

#         except Exception as e:
#             self.logger.error(f"{main.prefix_log}{traceback.format_exc()}")
#             raise e


# class LoopUpdateEdgesCombiner:
#     """Combiner class per LoopUpdateEdges"""

#     def setup(self, context):
#         """Setup del combiner"""
#         pass

#     def reduce(self, key: PairWritable, values: List[SpecialEdgeValueWritable], context) -> Iterator[Tuple[PairWritable, SpecialEdgeValueWritable]]:
#         """Funzione reduce del combiner"""
#         weight = 0.0
#         contain_interactions = False
#         other_values = []

#         for s in values:
#             if s.type == Settings.INTERACTION_TYPE:
#                 weight += s.value
#                 contain_interactions = True
#             else:
#                 other_values.append(s)

#         # Emetti tutti i valori non-interaction
#         for val in other_values:
#             yield key, val

#         # Se ci sono interazioni, emetti il totale
#         if contain_interactions:
#             spec = SpecialEdgeValueWritable()
#             spec.init(Settings.INTERACTION_TYPE, weight, -1, None)
#             yield key, spec

#     def cleanup(self, context):
#         """Cleanup del combiner"""
#         pass


# class LoopUpdateEdgesReducer:
#     """Reducer class per LoopUpdateEdges"""

#     def __init__(self):
#         self.logger = logging.getLogger(self.__class__.__name__)
#         self.mout = None
#         self.PRECISE = 0.0000001
#         self.round = ""
#         self.threshold = -1
#         self.window_size = -1
#         self.loop_index = 0

#     def setup(self, context, output_path: str, round_num: str, threshold: float, window_size: int):
#         """Setup del reducer"""
#         self.mout = MultipleOutputs(output_path)
#         self.round = round_num
#         self.threshold = threshold
#         self.window_size = window_size
#         self.loop_index = int(round_num)

#     def update_delta_window(self, key: PairWritable, delta: float, c_loop_round: int,
#                            window_size: int, delta_window: BitSet, threshold: float) -> float:
#         """
#         Aggiorna la finestra delta
#         """
#         # c_loop_round inizializzato a zero
#         index = c_loop_round % window_size

#         if delta < 0:
#             delta_window.clear(index)
#         else:
#             delta_window.set(index)

#         returned_delta = delta

#         if c_loop_round >= (window_size - 1):
#             # Scrivi il bitset completo
#             spec = SpecialEdgeTypeWritable()
#             spec.init(Settings.SLIDING, key.left, key.right, -1, -1, None, window_size, delta_window)
#             self.mout.write("updateEdge", spec, "NullWritable", "sliding/sliding")

#             # La finestra è piena ora
#             if delta_window.get(index):
#                 i_same_size = delta_window.cardinality()
#                 if i_same_size > threshold * window_size:
#                     returned_delta = 2
#             else:
#                 i_same_size = window_size - delta_window.cardinality()
#                 if i_same_size > threshold * window_size:
#                     returned_delta = -2
#         else:
#             # Scrivi da 0 a current_index del bitset
#             spec = SpecialEdgeTypeWritable()
#             spec.init(Settings.SLIDING, key.left, key.right, -1, -1, None, c_loop_round + 1, delta_window)
#             self.mout.write("updateEdge", spec, "NullWritable", "sliding/sliding")

#         return returned_delta

#     def reduce(self, key: PairWritable, values: List[SpecialEdgeValueWritable], context):
#         """Funzione reduce principale"""
#         try:
#             u = key.left
#             v = key.right

#             using_sliding_windows = self.window_size > 0

#             delta_di = 0.0
#             delta_ei = 0.0
#             delta_ci = 0.0
#             delta_t = 0.0
#             dis_uv = -1.0
#             exist_delta_t = False

#             # Solo per delta window
#             b_delta_window = BitSet(32)  # inizializza un set di 32 bit

#             for s in values:
#                 if s.type == Settings.EDGE_TYPE:
#                     dis_uv = s.value
#                 elif s.type == Settings.SLIDING:
#                     # inizializza b_delta_window
#                     for i in range(s.no_bits + 1):
#                         if s.sliding.get(i):
#                             b_delta_window.set(i)
#                     continue
#                 elif s.type == Settings.D_TYPE:
#                     delta_di += s.value
#                 elif s.type == Settings.C_TYPE:
#                     delta_ci += s.value
#                 elif s.type == Settings.E_TYPE:
#                     delta_ei += s.value
#                 elif s.type == Settings.INTERACTION_TYPE:
#                     delta_t += s.value
#                     exist_delta_t = True

#             # VALIDAZIONE DEL CODICE
#             if main.DEBUG:
#                 # Se DEBUG, la somma di DI, CI, EI deve essere uguale a delta_t
#                 assert abs(delta_t - (delta_di + delta_ci + delta_ei)) <= 1e-5, \
#                     "Sum of DI, CI, EI must equal to delta_t"
#             else:
#                 assert abs(delta_di) < 1e-5 and abs(delta_ci) < 1e-5 and abs(delta_ei) < 1e-5, \
#                     "DI, CI, EI must be zero if not debugged"

#             if dis_uv < 0:
#                 return

#             if 0 < dis_uv < 1:
#                 assert exist_delta_t, f"With non-converged edge ({u},{v}), there must be delta to update!!!"
#                 if using_sliding_windows:
#                     delta_t = self.update_delta_window(key, delta_t, self.loop_index,
#                                                      self.window_size, b_delta_window, self.threshold)
#             else:
#                 assert not exist_delta_t, f"With converged edge ({u},{v}), there is no delta emitted!!!"

#             d_t_1 = dis_uv + delta_t

#             if d_t_1 > 1:
#                 d_t_1 = 1.0
#             if d_t_1 < 0:
#                 d_t_1 = 0.0

#             if main.DEBUG:
#                 test = f",oldDis: {dis_uv:.8f}, newDis: {d_t_1:.8f}, DI: {delta_di:.8f}, EI: {delta_ei:.8f}, CI: {delta_ci:.8f}"
#                 self.mout.write("testing", key, test, f"test/test_DI_CI_EI_{self.round}")

#             if 0 < d_t_1 < 1:
#                 self.mout.write("signal", "Flag", "", "flag/flag")

#             spec = SpecialEdgeTypeWritable()
#             spec.init(Settings.EDGE_TYPE, key.left, key.right, d_t_1, -1, None, -1, None)
#             self.mout.write("updateEdge", spec, "NullWritable", "edges/edges")

#         except Exception as e:
#             self.logger.error(f"{main.prefix_log}{traceback.format_exc()}")
#             raise e
#         except AssertionError as e:
#             self.logger.error(f"{main.prefix_log}{traceback.format_exc()}")
#             raise e

#     def cleanup(self, context):
#         """Cleanup del reducer"""
#         if self.mout:
#             self.mout.close()


class MRUpdateEdges:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.sc = self.spark.sparkContext

    def mapReduce(self, rdd_dynamic_interactions, tau_, window_size_) -> int:
        tau = tau_
        window_size = window_size_

        def map_function(edge):
            if edge.type == "L":
                pass
            else:
                return (
                    Row(center=edge.center, target=edge.target),
                    Row(
                        type=edge.type,
                        weight=edge.weight,
                    ),
                )

        output = rdd_dynamic_interactions.map(map_function)
        output = output.groupByKey()

        return output
        # Validazione input
        # if main.DEBUG:
        #     assert len(input_files) == 4, "In case of DEBUG, there must be 4 input folders"
        # else:
        #     assert len(input_files) == 3, "In release case, there must be 3 input folders"

        # Qui dovresti implementare la logica per leggere i file di input
        # e processarli attraverso mapper, combiner e reducer

        # mapper = LoopUpdateEdgesMapper()
        # combiner = LoopUpdateEdgesCombiner()
        # reducer = LoopUpdateEdgesReducer()

        # # Setup del reducer
        # reducer.setup(None, output_files, round_num, threshold, window_size)
