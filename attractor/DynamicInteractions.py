from attractor.LoopDynamicInteractionsFasterNoCache import LoopDynamicInteractionsFasterNoCache
import math

@staticmethod
def is_in_set1(pu: int, a: int, b: int, c: int) -> bool:
    # Verify if pu is in the set {a, b, c}
    return pu == a or pu == b or pu == c

@staticmethod
def node2hash(u: int, no_partitions: int) -> int:
    return u % no_partitions

@staticmethod
def genkey3num(a: int, b: int, c: int) -> str:
    # Sort three numbers
    if a > c:
        a, c = c, a
    if a > b:
        a, b = b, a
    if b > c:
        b, c = c, b
    
    assert a < b < c, f"Sort error: {a} {b} {c}"
    return f"{a},{b},{c}"

@staticmethod
def check_equal(a: int, b: int, c: int) -> int:
    # Check in how many numbers are equal to each other. (a==b) , (b==c) and
    # (c==a). If result==0, all are different. If result==1, 2 same, 1
    # different. If result=3: all are same.
    x1 = 1 if a == b else 0
    x2 = 1 if b == c else 0
    x3 = 1 if a == c else 0
    s = x1 + x2 + x3
    return s

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
        # Three vertex of the same partition
        repeat_count = (p - 1) * (p - 2) // 2
    elif res == 1:
        # Two vertex of the same partition
        repeat_count = p - 2
    elif res == 0:
        # Three vertex of different partitions
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
    
    spec.init(Settings.INTERACTION_TYPE, ulab, vlab, delta_ulab_vlab, -1, None, -1, None)
    mout.write("attr", spec, null_writable, "delta_dis/delta_dis")