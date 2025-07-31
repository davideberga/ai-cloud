import numpy as np

class CleanUp:
    
    @staticmethod
    def reduce_edges(
    output_update_edges,
):
        non_converged = 0
        
        for row in output_update_edges:
            c, t = row[0].split("-")
            u = int(c)
            v = int(t)
            dis = row[1][2]
        
            if dis < 1 and dis > 0:
                u -= 1
                v -= 1
                non_converged += 1

        
        return non_converged

    def validate_final_edges(self, file1, no_original_edges):
        cnt = 0
        with open(file1, "r") as reader:
            for line in reader:
                args = line.strip().split()
                u = int(args[0])
                v = int(args[1])
                dis = float(args[2].replace(",", "."))

                assert abs(dis) < 1e-10 or abs(dis - 1.0) < 1e-10
                cnt += 1

        assert cnt == no_original_edges
        
        
    @staticmethod
    def reduce_edges_spark(n_vertices, rdd_edges):
        # Step 1: Mark edges as converged / non-converged
        # row: (u, [{"target": v, "weight": dis}])
        marked = rdd_edges.map(
            lambda row: (
                row[0],
                row[1][0]["target"],
                row[1][0]["weight"],
                1 if 0 < row[1][0]["weight"] < 1 else 0
            )
        )
        # Now each record: (u, v, weight, is_non_converged)

        # Step 2: Count converged/non-converged
        agg = marked.map(lambda x: (1, (x[3], 1 - x[3]))).reduce(
            lambda a, b: (a[0] + b[0], (a[1][0] + b[1][0], a[1][1] + b[1][1]))
        )
        # agg[1][0] = non_converged, agg[1][1] = converged
        non_converged = agg[1][0]



        return non_converged