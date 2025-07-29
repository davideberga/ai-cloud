import numpy as np

class CleanUp:
    
    @staticmethod
    def reduce_edges(
    n_vertices,
    output_update_edges,
):
        dirty = np.zeros((n_vertices))
        non_converged = 0
        converged = 0
        
        
        for row in output_update_edges:
            u = row.center
            v = row.target
            dis = row.weight
            if dis < 1 and dis > 0:
                u -= 1
                v -= 1
                dirty[u] = 1
                dirty[v] = 1
                non_converged += 1
            else:
                converged   += 1
                
        for row in output_update_edges:
            u = row.center
            v = row.target
            u -= 1
            v -= 1
            if dirty[u] == 1 or dirty[v] == 1:
                dirty[u] = 2 if dirty[u] == 0 else dirty[u]
                dirty[v] = 2 if dirty[v] == 0 else dirty[v]
                
        the_number_of_continued_to_used_edges = 0
                
        for row in output_update_edges:
            u = row.center
            v = row.target
            u -= 1
            v -= 1
            if dirty[u] == 1 or dirty[v] == 1 or dirty[u] == 2 or dirty[v] == 2:
                the_number_of_continued_to_used_edges += 1
            else:
                pass
        
        return converged, non_converged, the_number_of_continued_to_used_edges, output_update_edges

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