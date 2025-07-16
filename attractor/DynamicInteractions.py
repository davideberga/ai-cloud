import math
from typing import Dict, List

class DynamicInteractions:
    @staticmethod
    def is_in_set1(pu: int, a: int, b: int, c: int) -> bool:
        # Verify if pu is in the set {a, b, c}
        return pu == a or pu == b or pu == c

    @staticmethod
    def node2hash(u: int, no_partitions: int) -> int:
        return u % no_partitions

    @staticmethod
    def check_equal(p_u: int, p_v: int, p_c: int) -> int:
        # Check in how many numbers are equal to each other. (a==b) , (b==c) and
        # (c==a). If result==0, all are different. If result==1, 2 same, 1
        # different. If result=3: all are same.
        x1 = 1 if p_u == p_v else 0
        x2 = 1 if p_v == p_c else 0
        x3 = 1 if p_u == p_c else 0
        s = x1 + x2 + x3
        return s

    def compute_ci(
        u: int,
        v: int,
        c: int,
        deg_u: int,
        deg_v: int,
        dis_u_v: float,
        dis_u_c: float,
        dis_v_c: float,
        n_partitions: int,
    ) -> float:
        assert 0 < dis_u_v < 1, "This arc is already convergent!!!"

        w1 = 1 - dis_u_c
        w2 = 1 - dis_v_c
        ci = -w2 * math.sin(w1) / deg_u - w1 * math.sin(w2) / deg_v

        p_u = DynamicInteractions.node2hash(u, n_partitions)
        p_v = DynamicInteractions.node2hash(v, n_partitions)
        p_c = DynamicInteractions.node2hash(c, n_partitions)

        repeat_count = 1
        res = DynamicInteractions.check_equal(p_u, p_v, p_c)

        if res == 3:
            # Three vertex of the same partition
            repeat_count = (n_partitions - 1) * (n_partitions - 2) // 2
        elif res == 1:
            # Two vertex of the same partition
            repeat_count = n_partitions - 2
        elif res == 0:
            # Three vertex of different partitions
            repeat_count = 1

        ci = ci / repeat_count
        return ci

    @staticmethod
    def compute_ei(
        u: int,
        v: int,
        middle: int,
        dis_u_middle: float,
        dis_v_middle: float,
        deg_middle: int,
        n_partitions: int,
        adjListDictForExclusive: Dict[int, List],
        dictSumWeight: Dict[int, float],
        lambda_: float,
        graph_key: List[int],
    ) -> float:
        """
        Calcola l'effetto esclusivo del nodo u sull'arco (middle, v)
        """
        if u == v:
            return 0.0

        vartheta_uv = 0
        neighbors_u = adjListDictForExclusive.get(u, [])
        neighbors_v = adjListDictForExclusive.get(v, [])

        i, j = 0, 0
        len_neigh_u, len_neigh_v = len(neighbors_u), len(neighbors_v)
        sum_common_weight = 0.0
        common_main_node = 0

        while i < len_neigh_u and j < len_neigh_v:
            first = neighbors_u[i]
            second = neighbors_v[j]

            if first.vertex_id < second.vertex_id:
                i += 1
            elif second.vertex_id < first.vertex_id:
                j += 1
            else:
                sum_common_weight += (1 - first.weight) + (1 - second.weight)
                i += 1
                j += 1

                p_u = DynamicInteractions.node2hash(first.vertex_id, n_partitions)
                if p_u in partition_name_splitted:
                    common_main_node += 1

                if DynamicInteractions.is_in_set1(
                    DynamicInteractions.node2hash(first.vertex_id, n_partitions),
                    graph_key[0],
                    graph_key[1],
                    graph_key[2],
                ):
                    common_main_node += 1

        vartheta_uv = sum_common_weight / (dictSumWeight[u] + dictSumWeight[v])

        rho_uv = vartheta_uv
        if vartheta_uv < lambda_:
            rho_uv = vartheta_uv - lambda_

        ei = -rho_uv * math.sin(1 - dis_u_middle) / deg_middle

        p_u = DynamicInteractions.node2hash(u, n_partitions)
        p_v = DynamicInteractions.node2hash(v, n_partitions)
        p_c = DynamicInteractions.node2hash(middle, n_partitions)

        repeat_count = 1
        res = DynamicInteractions.check_equal(p_u, p_v, p_c)

        if res == 3:
            repeat_count = (n_partitions - 1) * (n_partitions - 2) // 2
        elif res == 1:
            repeat_count = n_partitions - 2
        elif res == 0:
            repeat_count = 1

        ei /= repeat_count
        return ei

    @staticmethod
    def compute_di(
        p_u: int, p_v: int, p: int, distance_u_v: float, deg_u: int, deg_v: int
    ) -> float:
        """Calcola DI dell'arco non convergente (u,v) con ridimensionamento"""

        assert p >= 3, "Il numero di partizioni deve essere >= 3"
        assert 0 < distance_u_v < 1, f"La distanza di (u, v) deve essere in (0, 1)"

        di = -math.sin(1 - distance_u_v) / deg_u - math.sin(1 - distance_u_v) / deg_v
        scale = (p - 1) * (p - 2) // 2 if p_u == p_v else p - 2
        return di / scale

    @staticmethod
    def union_intersection(
        u: int, # center
        v , # neighbor
        deg_u: int,
        deg_v: int,
        adjListDictMain: Dict[int, List],
        adjListDictForExclusive: Dict[int, List],
        dictSumWeight: Dict[int, float],
        n_partitions: int, # number of partitions
        duv: float,
        graph_key_partitions: List[int], # star graph?
        partition_name_splitted: List[int],
        lambda_: float,
    ):
        """
        Calcola DI, CI, EI per l'arco (ulab, vlab) nel sottografo graph_key
        """
        if duv < 0 or duv > 1:
            return

        neighbors_u = adjListDictMain.get(u, [])
        neighbors_v = adjListDictMain.get(v, [])

        i, j = 0, 0
        len_neigh_u, len_neigh_v = len(neighbors_u), len(neighbors_v)
        sum_ci = 0
        sum_ei = 0

        # Already computed in MRDynamicInteractions.py
        #di = DynamicInteractions.compute_di(u, v, n_partitions, duv, deg_u, deg_v)

        while i < len_neigh_u and j < len_neigh_v:
            first = neighbors_u[i]
            second = neighbors_v[j]

            p_u = DynamicInteractions.node2hash(u, no_partitions=n_partitions)
            if p_u in partition_name_splitted:
                main_edge_first = True

            if p_u in partition_name_splitted:
                main_edge_second = True

            assert main_edge_first and main_edge_second, (
                "C'è un arco posteriore in questo caso!!!"
            )

            if first.vertex_id < second.vertex_id:
                sum_ei += DynamicInteractions.compute_ei(
                    first.vertex_id,
                    v,
                    u,
                    first.weight,
                    duv,
                    deg_u,
                    n_partitions,
                    adjListDictForExclusive,
                    dictSumWeight,
                    lambda_,
                    graph_key_partitions,
                )
                i += 1
            elif second.vertex_id < first.vertex_id:
                sum_ei += DynamicInteractions.compute_ei(
                    second.vertex_id,
                    u,
                    v,
                    second.weight,
                    duv,
                    deg_v,
                    n_partitions,
                    adjListDictForExclusive,
                    dictSumWeight,
                    lambda_,
                    graph_key_partitions,
                )
                j += 1
            else:
                common_node = first.vertex_id
                sum_ci += DynamicInteractions.compute_ci(
                    u, v, common_node, deg_u, deg_v, duv, first.weight, second.weight, n_partitions
                )
                i += 1
                j += 1

        # Processa i rimanenti vicini di u
        while i < len_neigh_u:
            first = neighbors_u[i]
            sum_ei += DynamicInteractions.compute_ei(
                first.vertex_id,
                v,
                u,
                first.weight,
                duv,
                deg_u,
                n_partitions,
                adjListDictForExclusive,
                dictSumWeight,
                lambda_,
                graph_key_partitions,
            )
            i += 1

        # Processa i rimanenti vicini di v
        while j < len_neigh_v:
            second = neighbors_v[j]
            sum_ei += DynamicInteractions.compute_ei(
                second.vertex_id,
                u,
                v,
                second.weight,
                duv,
                deg_v,
                n_partitions,
                adjListDictForExclusive,
                dictSumWeight,
                lambda_,
                graph_key_partitions,
            )
            j += 1

        return sum_ci + sum_ei

        # null_writable = None  # Equivalente di NullWritable.get()
        # spec = SpecialEdgeTypeWritable()

        # spec.init(
        #     Settings.INTERACTION_TYPE, ulab, vlab, delta_ulab_vlab, -1, None, -1, None
        # )
        # mout.write("attr", spec, null_writable, "delta_dis/delta_dis")
