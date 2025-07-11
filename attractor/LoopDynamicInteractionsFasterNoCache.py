import sys
import math
import traceback
from typing import Dict, List, Tuple, Optional, Any
import logging

# Assumendo che queste classi siano implementate in altri file
from writable.EdgeValueWritable import EdgeValueWritable
from writable.PairWritable import PairWritable
from writable.Settings import Settings
from writable.SpecialEdgeTypeWritable import SpecialEdgeTypeWritable
from writable.StarGraphWithPartitionWritable import StarGraphWithPartitionWritable
from writable.StarGraphWritable import StarGraphWritable
from writable.TripleWritable import TripleWritable
from attractor.AttrUtils import AttrUtils
import main
from writable.NeighborWritable import NeighborWritable

class LoopDynamicInteractionsFasterNoCache:
    """
    Traduzione Python della classe Java LoopDynamicInteractionsFasterNoCache
    per il calcolo di interazioni dinamiche in grafi usando MapReduce
    """
    
    jobname = "DynamicInteractionNoCaching"
    
    def __init__(self):
        self.global_job = None
        self.log_map = logging.getLogger(f"{self.__class__.__name__}.Map")
        self.log_reduce = logging.getLogger(f"{self.__class__.__name__}.Reduce")
    
    class Adj:
        """Classe per rappresentare un nodo adiacente"""
        def __init__(self, v_: int, deg: int, dis: float):
            self.lab = v_
            self.deg = deg
            self.dis = dis
        
        def __lt__(self, other: 'LoopDynamicInteractionsFasterNoCache.Adj') -> bool:
            return self.lab < other.lab
        
        def __eq__(self, other: object) -> bool:
            if not isinstance(other, LoopDynamicInteractionsFasterNoCache.Adj):
                return False
            return self.lab == other.lab
        
        def __gt__(self, other: 'LoopDynamicInteractionsFasterNoCache.Adj') -> bool:
            return self.lab > other.lab

    class Edge:
        """Classe per rappresentare un arco"""
        def __init__(self, u: int, v: int, dis: float):
            self.u = u
            self.v = v
            self.dis = dis

    @staticmethod
    def is_in_set1(pu: int, a: int, b: int, c: int) -> bool:
        """Verifica se pu è in {a, b, c}"""
        return pu == a or pu == b or pu == c

    @staticmethod
    def node2hash(u: int, no_partitions: int) -> int:
        """Assegna un nodo a una partizione. O(1)"""
        return u % no_partitions

    @staticmethod
    def genkey3num(a: int, b: int, c: int) -> str:
        """Genera una chiave ordinata da tre numeri"""
        # Ordina i tre numeri
        if a > c:
            a, c = c, a
        if a > b:
            a, b = b, a
        if b > c:
            b, c = c, b
        
        assert a < b < c, f"Errore nell'ordinamento di tre numeri: {a} {b} {c}"
        return f"{a},{b},{c}"

    @staticmethod
    def check_equal(a: int, b: int, c: int) -> int:
        """
        Conta quanti numeri sono uguali tra loro.
        Ritorna 0 se tutti diversi, 1 se due uguali, 3 se tutti uguali
        """
        x1 = 1 if a == b else 0
        x2 = 1 if b == c else 0
        x3 = 1 if a == c else 0
        s = x1 + x2 + x3
        assert s in [0, 1, 3], f"Ci sono solo tre situazioni s=0,1 o 3: qui: {s}"
        return s

    class Map:
        """Classe Mapper"""
        
        def __init__(self):
            self.p = 0
            self.map_deg: Dict[int, int] = {}
            self.dict_load_balance: Dict[str, int] = {}
            self.global_job: Optional[Any] = None
        
        def setup(self, context: Any) -> None:
            """Setup del mapper"""
            self.p = context.get_configuration().get_int("ro", 0)
            conf = context.get_configuration()
            self.map_deg = AttrUtils.read_deg_map(conf, main.degree_file_key)
            
            # if MasterMR.ESTIMATION_WORK_LOAD:
            #     self.dict_load_balance = self.read_balanced_file(
            #         conf.get(MasterMR.LOAD_BALANCED_HEADER), conf
            #     )

        def map(self, key: Any, value: StarGraphWithPartitionWritable, context: Any) -> None:
            """
            Funzione di mapping principale
            """
            try:
                star_graph_writable = StarGraphWritable()
                
                center = value.center
                deg_center = self.map_deg[center]
                
                star_graph_writable.set(center, deg_center, value.neighbors)
                
                for trip in value.triple_sub_graphs:
                    if main.ESTIMATION_WORK_LOAD:
                        if str(trip) not in self.dict_load_balance:
                            return
                    context.write(trip, star_graph_writable)
                    
            except Exception as e:
                traceback.print_exc()
                logging.getLogger().error(f"{main.prefix_log}{traceback.format_exc()}")
                if self.global_job:
                    self.global_job.kill_job()

        def read_balanced_file(self, balanced_file: str, conf: Any) -> Dict[str, int]:
            """Legge il file di bilanciamento del carico"""
            try:
                dict_load_balance: Dict[str, int] = {}
                # Implementazione specifica per leggere dal filesystem
                # (dipende dal framework utilizzato)
                return dict_load_balance
            except Exception as e:
                traceback.print_exc()
                return {}

    @staticmethod
    def compute_ci(u: int, v: int, c: int, du: int, dv: int,
                   dis_u_v: float, dis_u_c: float, dis_v_c: float, p: int) -> float:
        """Calcola CI(u,v)"""
        assert 0 < dis_u_v < 1, "Questo arco è già convergente!!!"
        
        w1 = 1 - dis_u_c
        w2 = 1 - dis_v_c
        ci = -w2 * math.sin(w1) / du - w1 * math.sin(w2) / dv
        
        pu = LoopDynamicInteractionsFasterNoCache.node2hash(u, p)
        pv = LoopDynamicInteractionsFasterNoCache.node2hash(v, p)
        pc = LoopDynamicInteractionsFasterNoCache.node2hash(c, p)
        
        repeat_count = 1
        res = LoopDynamicInteractionsFasterNoCache.check_equal(pu, pv, pc)
        
        if res == 3:
            # Tre vertici nella stessa partizione
            repeat_count = (p - 1) * (p - 2) // 2
        elif res == 1:
            # Due vertici nella stessa partizione
            repeat_count = p - 2
        elif res == 0:
            # Tre vertici in tre partizioni diverse
            repeat_count = 1
        
        ci = ci / repeat_count
        return ci

    @staticmethod
    def compute_ei_with_cache(u: int, v: int, middle: int,
                             dis_u_middle: float, dis_v_middle: float, deg_middle: int, p: int,
                             adj_list_dict: Dict[int, List[NeighborWritable]],
                             dict_sum_weight: Dict[int, float], lambda_param: float,
                             graph_key: List[int]) -> float:
        """
        Calcola l'effetto esclusivo del nodo u sull'arco (middle, v)
        """
        if u == v:
            return 0.0
        
        vartheta_uv = 0
        neighbors_u = adj_list_dict.get(u, [])
        neighbors_v = adj_list_dict.get(v, [])
        
        i, j = 0, 0
        m, n = len(neighbors_u), len(neighbors_v)
        sum_common_weight = 0.0
        common_main_node = 0
        
        while i < m and j < n:
            first = neighbors_u[i]
            second = neighbors_v[j]
            
            if first.lab < second.lab:
                i += 1
            elif second.lab < first.lab:
                j += 1
            else:
                sum_common_weight += (1 - first.dis) + (1 - second.dis)
                i += 1
                j += 1
                if LoopDynamicInteractionsFasterNoCache.is_in_set1(
                    LoopDynamicInteractionsFasterNoCache.node2hash(first.lab, p),
                    graph_key[0], graph_key[1], graph_key[2]
                ):
                    common_main_node += 1
        
        vartheta_uv = sum_common_weight / (dict_sum_weight[u] + dict_sum_weight[v])
        
        rho_uv = vartheta_uv
        if vartheta_uv < lambda_param:
            rho_uv = vartheta_uv - lambda_param
        
        ei = -rho_uv * math.sin(1 - dis_u_middle) / deg_middle
        
        pu = LoopDynamicInteractionsFasterNoCache.node2hash(u, p)
        pv = LoopDynamicInteractionsFasterNoCache.node2hash(v, p)
        pc = LoopDynamicInteractionsFasterNoCache.node2hash(middle, p)
        
        repeat_count = 1
        res = LoopDynamicInteractionsFasterNoCache.check_equal(pu, pv, pc)
        
        if res == 3:
            repeat_count = (p - 1) * (p - 2) // 2
        elif res == 1:
            repeat_count = p - 2
        elif res == 0:
            repeat_count = 1
        
        ei /= repeat_count
        return ei

    @staticmethod
    def compute_di(u: int, v: int, p: int, duv: float, deg_u: int, deg_v: int) -> float:
        """Calcola DI dell'arco non convergente (u,v) con ridimensionamento"""
        assert p >= 3, "Il numero di partizioni deve essere >= 3"
        assert 0 < duv < 1, f"La distanza di ({u}, {v}) deve essere in (0, 1)"
        
        di = -math.sin(1 - duv) / deg_u - math.sin(1 - duv) / deg_v
        
        if LoopDynamicInteractionsFasterNoCache.node2hash(u, p) == \
           LoopDynamicInteractionsFasterNoCache.node2hash(v, p):
            scale = (p - 1) * (p - 2) // 2
            di /= scale
        else:
            scale = p - 2
            di /= scale
        
        return di

    @staticmethod
    def union_intersection(ulab: int, vlab: int, deg_u: int, deg_v: int,
                          adj_list_main_dict: Dict[int, List[NeighborWritable]],
                          adj_list_dict_for_exclusive: Dict[int, List[NeighborWritable]],
                          dict_sum_weight: Dict[int, float], p: int, duv: float,
                          graph_key: str, graph_key_partitions: List[int],
                          lambda_param: float, mout: Any) -> None:
        """
        Calcola DI, CI, EI per l'arco (ulab, vlab) nel sottografo graph_key
        """
        if duv < 0 or duv > 1:
            return
        
        neighbors_u = adj_list_main_dict.get(ulab, [])
        neighbors_v = adj_list_main_dict.get(vlab, [])
        
        i, j = 0, 0
        m, n = len(neighbors_u), len(neighbors_v)
        sum_ci = 0
        sum_ei = 0
        
        # Calcola DI con ridimensionamento
        di = LoopDynamicInteractionsFasterNoCache.compute_di(
            ulab, vlab, p, duv, deg_u, deg_v
        )
        
        while i < m and j < n:
            first = neighbors_u[i]
            second = neighbors_v[j]
            
            # Verifica che siano archi principali
            main_edge_first = LoopDynamicInteractionsFasterNoCache.is_in_set1(
                LoopDynamicInteractionsFasterNoCache.node2hash(first.lab, p),
                graph_key_partitions[0], graph_key_partitions[1], graph_key_partitions[2]
            )
            main_edge_second = LoopDynamicInteractionsFasterNoCache.is_in_set1(
                LoopDynamicInteractionsFasterNoCache.node2hash(second.lab, p),
                graph_key_partitions[0], graph_key_partitions[1], graph_key_partitions[2]
            )
            
            assert main_edge_first and main_edge_second, "C'è un arco posteriore in questo caso!!!"
            
            if first.lab < second.lab:
                sum_ei += LoopDynamicInteractionsFasterNoCache.compute_ei_with_cache(
                    first.lab, vlab, ulab, first.dis, duv, deg_u, p,
                    adj_list_dict_for_exclusive, dict_sum_weight, lambda_param,
                    graph_key_partitions
                )
                i += 1
            elif second.lab < first.lab:
                sum_ei += LoopDynamicInteractionsFasterNoCache.compute_ei_with_cache(
                    second.lab, ulab, vlab, second.dis, duv, deg_v, p,
                    adj_list_dict_for_exclusive, dict_sum_weight, lambda_param,
                    graph_key_partitions
                )
                j += 1
            else:
                common_node = first.lab
                sum_ci += LoopDynamicInteractionsFasterNoCache.compute_ci(
                    ulab, vlab, common_node, deg_u, deg_v, duv,
                    first.dis, second.dis, p
                )
                i += 1
                j += 1
        
        # Processa i rimanenti vicini di u
        while i < m:
            first = neighbors_u[i]
            sum_ei += LoopDynamicInteractionsFasterNoCache.compute_ei_with_cache(
                first.lab, vlab, ulab, first.dis, duv, deg_u, p,
                adj_list_dict_for_exclusive, dict_sum_weight, lambda_param,
                graph_key_partitions
            )
            i += 1
        
        # Processa i rimanenti vicini di v
        while j < n:
            second = neighbors_v[j]
            sum_ei += LoopDynamicInteractionsFasterNoCache.compute_ei_with_cache(
                second.lab, ulab, vlab, second.dis, duv, deg_v, p,
                adj_list_dict_for_exclusive, dict_sum_weight, lambda_param,
                graph_key_partitions
            )
            j += 1
        
        delta_ulab_vlab = di + sum_ci + sum_ei
        
        null_writable = None  # Equivalente di NullWritable.get()
        spec = SpecialEdgeTypeWritable()
        
        if main.DEBUG:
            # Per scopi di debug
            spec.init(Settings.D_TYPE, ulab, vlab, di, -1, None, -1, None)
            mout.write("attr", spec, null_writable, "debug/debug")
            
            spec.init(Settings.C_TYPE, ulab, vlab, sum_ci, -1, None, -1, None)
            mout.write("attr", spec, null_writable, "debug/debug")
            
            spec.init(Settings.E_TYPE, ulab, vlab, sum_ei, -1, None, -1, None)
            mout.write("attr", spec, null_writable, "debug/debug")
        
        spec.init(Settings.INTERACTION_TYPE, ulab, vlab, delta_ulab_vlab, -1, None, -1, None)
        mout.write("attr", spec, null_writable, "delta_dis/delta_dis")

    class Reduce:
        """Classe Reducer"""
        
        def __init__(self):
            self.mout: Optional[Any] = None
            self.lambda_param = 0.5
            self.p = 0
            self.task_attempt_id: Optional[Any] = None
            self.task_id: Optional[Any] = None
            self.reducer_number = -1
            self.map_deg: Dict[int, int] = {}
            self.global_job: Optional[Any] = None
        
        def setup(self, context: Any) -> None:
            """Setup del reducer"""
            self.mout = context.get_multiple_outputs()
            self.lambda_param = context.get_configuration().get_double("lambda", 0.5)
            self.p = context.get_configuration().get_int("ro", 0)
            
            # Ottieni informazioni sul task
            self.task_attempt_id = context.get_task_attempt_id()
            self.task_id = self.task_attempt_id.get_task_id()
            self.reducer_number = self.task_id.get_id()
            
            conf = context.get_configuration()
            self.map_deg = AttrUtils.read_deg_map(conf, main.degree_file_key)
        
        def reduce(self, subgraph_key: TripleWritable, star_graphs: Any, context: Any) -> None:
            """
            Ogni reducer gestirà un sottografo G_{ijk}
            """
            try:
                components = [subgraph_key.left, subgraph_key.mid, subgraph_key.right]
                
                # Dizionario dei vicini originali per interazioni esclusive
                adj_list_dict_for_exclusive: Dict[int, List[NeighborWritable]] = {}
                
                # Dizionario solo dei vicini principali
                adj_list_dict_main: Dict[int, List[NeighborWritable]] = {}
                
                # Lista degli archi, visiteremo ogni arco principale una volta
                list_edges: List[LoopDynamicInteractionsFasterNoCache.Edge] = []
                
                dict_sum_weight: Dict[int, float] = {}
                
                # Per investigazione della skewness
                main_edges = 0
                rear_edges = 0
                sum_degree = 0
                
                for star in star_graphs:
                    center = star.center
                    
                    assert LoopDynamicInteractionsFasterNoCache.is_in_set1(
                        LoopDynamicInteractionsFasterNoCache.node2hash(center, self.p),
                        components[0], components[1], components[2]
                    ), f"Il nodo {center} deve essere nel sottografo {subgraph_key} con p={self.p}"
                    
                    deg_center = star.deg_center
                    
                    if main.CheckDegree:
                        assert len(star.neighbors) == deg_center, \
                            "Il numero di vicini non corrisponde al grado del centro"
                    
                    sum_degree += deg_center
                    sum_weight = 0.0
                    
                    for neighbor_info in star.neighbors:
                        neighbor = neighbor_info.lab
                        dis = neighbor_info.dis
                        sum_weight += (1.0 - dis)
                        adj = neighbor_info
                        
                        # Costruzione del dizionario degli archi principali
                        if LoopDynamicInteractionsFasterNoCache.is_in_set1(
                            LoopDynamicInteractionsFasterNoCache.node2hash(neighbor, self.p),
                            components[0], components[1], components[2]
                        ):
                            # Evita di aggiungere archi duplicati
                            if center > neighbor:
                                if 0 < dis < 1:
                                    edge = LoopDynamicInteractionsFasterNoCache.Edge(
                                        center, neighbor, dis
                                    )
                                    list_edges.append(edge)
                                main_edges += 1
                            
                            if center in adj_list_dict_main:
                                adj_list_dict_main[center].append(adj)
                            else:
                                adj_list_dict_main[center] = [adj]
                        else:
                            rear_edges += 1
                        
                        # Costruzione del dizionario per interazioni esclusive
                        if center in adj_list_dict_for_exclusive:
                            adj_list_dict_for_exclusive[center].append(adj)
                        else:
                            adj_list_dict_for_exclusive[center] = [adj]
                    
                    assert center not in dict_sum_weight, "Grafo stellare duplicato in un sottografo!!!"
                    dict_sum_weight[center] = sum_weight
                
                # Processa ogni arco principale
                for edge in list_edges:
                    assert LoopDynamicInteractionsFasterNoCache.is_in_set1(
                        LoopDynamicInteractionsFasterNoCache.node2hash(edge.u, self.p),
                        components[0], components[1], components[2]
                    ), "Errore: non è un vertice principale"
                    
                    assert LoopDynamicInteractionsFasterNoCache.is_in_set1(
                        LoopDynamicInteractionsFasterNoCache.node2hash(edge.v, self.p),
                        components[0], components[1], components[2]
                    ), "Errore: non è un vertice principale"
                    
                    if 0 < edge.dis < 1:
                        deg_u = self.map_deg[edge.u]
                        deg_v = self.map_deg[edge.v]
                        LoopDynamicInteractionsFasterNoCache.union_intersection(
                            edge.u, edge.v, deg_u, deg_v,
                            adj_list_dict_main, adj_list_dict_for_exclusive,
                            dict_sum_weight, self.p, edge.dis,
                            str(subgraph_key), components, self.lambda_param,
                            self.mout
                        )
                
            except Exception as e:
                traceback.print_exc()
                logging.error(f"{main.prefix_log}{traceback.format_exc()}")
                if self.global_job:
                    self.global_job.kill_job()
        
        def cleanup(self, context: Any) -> None:
            """Cleanup del reducer"""
            if self.mout:
                self.mout.close()

    def run(self, args: List[str]) -> int:
        """Metodo principale per eseguire il job"""
        assert len(args) == 10, f"Il numero di argomenti deve essere 10, ricevuti {len(args)}"
        
        # Configurazione del job (la configurazione specifica dipenderà dal framework utilizzato)
        # Ad esempio, se si usa PyDoop, Snakebite, o un altro framework Python per Hadoop
        
        input_files = args[0]
        output_files = args[1]
        ro = int(args[2])
        lambda_param = float(args[3])
        loop_info = args[4]
        cache_size = int(args[5])
        reduce_memory_mb = args[6]
        num_reduce_tasks = int(args[7])
        degree_file = args[8]
        load_balanced_header = args[9]
        
        # Configurazione del job
        job_config = {
            'ro': ro,
            'lambda': lambda_param,
            'cacheSize': cache_size,
            'reduce_memory_mb': reduce_memory_mb,
            'map_memory_mb': '3072',
            'num_reduce_tasks': num_reduce_tasks,
            main.degree_file_key: degree_file,
            main.LOAD_BALANCED_HEADER: load_balanced_header
        }
        
        print(f"Eseguendo job: {self.jobname} Loop-{loop_info}")
        print(f"Input: {input_files}")
        print(f"Output: {output_files}")
        print(f"Configurazione: {job_config}")
        
        # Qui andrebbero le chiamate specifiche al framework MapReduce utilizzato
        # Ad esempio, configurazione di job Hadoop tramite PyDoop o simili
        
        return 1


# # Esempio di utilizzo
# if __name__ == "__main__":
#     if len(sys.argv) != 11:  # 10 parametri + nome script
#         print("Uso: python script.py <input> <output> <ro> <lambda> <loop> <cache_size> <reduce_memory> <num_reducers> <degree_file> <load_balanced_file>")
#         sys.exit(1)
    
#     job = LoopDynamicInteractionsFasterNoCache()
#     result = job.run(sys.argv[1:])
#     sys.exit(result)