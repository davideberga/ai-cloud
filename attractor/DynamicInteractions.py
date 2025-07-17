import math
from typing import Dict, List
from pyspark.sql.types import Row


class DynamicInteractions:
    @staticmethod
    def is_in_set1(p_u: int, i: int, j: int, k: int) -> bool:
        # Verify if pu is in the partition {a, b, c}
        return p_u == i or p_u == j or p_u == k

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

    # Common Interaction (CI) computation
    # Input: d(u,c), d(v,c), deg(u), deg(v) — where c are the common neighbors between nodes u and v (intersection).
    # Output: If u and v have many common neighbors, it is likely that they belong to the same community.
    # The result should be the sum of CI c(u,v).
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
        assert 0 < dis_u_v < 1 # This arc is already convergent
        print("SONO IN compute_ci")
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
    
    # Exclusive Interaction (EI) computation
    # Input: d(u,y), d(v,x), deg(u), deg(v) — where x is an exclusive neighbor of v and y is an exclusive neighbor of u.
    # ρ(x, u) measures how similar x (exclusive neighbor of v) is to u, even if x is not connected to u.
    # Output: If the exclusive neighbors of u and v are very similar to each other, it is likely that u and v belong to the same community.
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
        partition_name_splitted: List[int],
    ) -> float:
        
        print("SONO IN compute_ei")
        for chiave, valore in dictSumWeight.items():
            print(f"{chiave}: {valore}")

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

                if (
                    DynamicInteractions.node2hash(first.vertex_id, n_partitions)
                    in partition_name_splitted
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

    # Direct Interaction (DI) computation
    # Input: d(u,v) → Jaccard, deg(u), deg(v)
    # Output: Compute the attraction strength between u and v, i.e., how strong the pull is to bring them together.
    # The smaller d(u,v) is, the more similar the two nodes are, and the higher the attraction strength.
    # Everything is therefore computed based on the direct connection between u and v.

    @staticmethod
    def compute_di(
        p_u: int, p_v: int, n_partitions: int, distance_u_v: float, deg_u: int, deg_v: int
    ) -> float:

        assert n_partitions >= 3
        assert 0 < distance_u_v < 1
        print("SONO IN compute_di")
        di = -math.sin(1 - distance_u_v) / deg_u - math.sin(1 - distance_u_v) / deg_v
        scale = (n_partitions - 1) * (n_partitions - 2) // 2 if p_u == p_v else n_partitions - 2
        return di / scale

    # Compute DI, CI, and EI for the edge (u, v) in the subgraph graph_key.
    @staticmethod
    def union_intersection(
        u: int,  # center
        v,  # neighbor
        deg_u: int,
        deg_v: int,
        adjListDictMain: Dict[int, List],
        adjListDictForExclusive: Dict[int, List],
        dictSumWeight: Dict[int, float],
        n_partitions: int,  # number of partitions
        duv: float,
        partition_name_splitted: List[int],
        lambda_: float,
    ):
        if duv < 0 or duv > 1:
            return

        neighbors_u = adjListDictMain.get(u, [])
        neighbors_v = adjListDictMain.get(v, [])

        i, j = 0, 0
        len_neigh_u, len_neigh_v = len(neighbors_u), len(neighbors_v)
        sum_ci = 0
        sum_ei = 0

        di = DynamicInteractions.compute_di(u, v, n_partitions, duv, deg_u, deg_v)

        while i < len_neigh_u and j < len_neigh_v:
            first = neighbors_u[i]
            second = neighbors_v[j]

            first_id = first.vertex_id
            second_id = second.vertex_id

            p_first = DynamicInteractions.node2hash(
                first_id, no_partitions=n_partitions
            )
            p_second = DynamicInteractions.node2hash(
                second_id, no_partitions=n_partitions
            )

            assert (
                p_first in partition_name_splitted
                and p_second in partition_name_splitted
            ) # There is a rear edge

            condition = first_id < second_id
            vertex_ei = first if condition else second
            deg_ei = deg_u if condition else deg_v

            if first_id != second_id:
                print("Computing EI (row 221)")
                sum_ei += DynamicInteractions.compute_ei(
                    vertex_ei.vertex_id,
                    v,
                    u,
                    vertex_ei.weight,
                    duv,
                    deg_ei,
                    n_partitions,
                    adjListDictForExclusive,
                    dictSumWeight,
                    lambda_,
                    partition_name_splitted,
                )
                if condition:
                    i += 1
                else:
                    j += 1
            else:
                print("Computing CI (row 240)")
                sum_ci += DynamicInteractions.compute_ci(
                    u,
                    v,
                    first_id,
                    deg_u,
                    deg_v,
                    duv,
                    first.weight,
                    second.weight,
                    n_partitions,
                )
                i += 1
                j += 1

        # Process i remaining neighbors of u
        while i < len_neigh_u:
            u_neighbour = neighbors_u[i]
            print("Computing EI (row 258)")
            sum_ei += DynamicInteractions.compute_ei(
                u_neighbour.vertex_id,
                v,
                u,
                u_neighbour.weight,
                duv,
                deg_u,
                n_partitions,
                adjListDictForExclusive,
                dictSumWeight,
                lambda_,
                partition_name_splitted,
            )
            i += 1

        # Process j remaining neighbors of v
        while j < len_neigh_v:
            v_neighbour = neighbors_v[j]
            print("Computing EI (row 277)")
            sum_ei += DynamicInteractions.compute_ei(
                v_neighbour.vertex_id,
                u,
                v,
                v_neighbour.weight,
                duv,
                deg_v,
                n_partitions,
                adjListDictForExclusive,
                dictSumWeight,
                lambda_,
                partition_name_splitted,
            )
            j += 1
            
        delta =  di + sum_ci + sum_ei

        return Row(edge='attr', type='I', source=u, target=v, weight=delta)
