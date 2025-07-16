import time
from libs.Graph import Graph
from libs.Settings import Settings


class GraphUtils:
    """
    First component: computing Jaccard Distance in Single Machine.
    The output of this component will be used for Dynamic Interactions on MapReduce.
    """
    
    def __init__(self, num_vertices: int):
        """
        Initialize JaccardInit with graph parameters.
        
        Args:
            graph_file: Path to the graph file
            num_vertices: Number of vertices in the graph
        """
        self.m_i_current_step = 0
        self.cnt_vertices = num_vertices
        self.current_loops = 0
    
    def setup_graph(self, graph_file: str) -> Graph:
        """
        Create graph from file
        
        Args:
            str_file_name: Path to the graph file
        """
        loaded_graph = Graph()
        
        with open(graph_file, 'r') as file:
            for line_num, line in enumerate(file):
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) < 2:
                    continue
                
                try:            
                    vertex_start = int(parts[0])
                    vertex_end = int(parts[1])
                    loaded_graph.add_edge(vertex_start, vertex_end, d_weight=0.0)
                except ValueError as e:
                    print(f"Error parsing line {line_num + 1}: {line}")
                    raise ValueError(f"Invalid number format in line {line_num + 1}") from e
        
        # Assertions to verify graph construction
        assert len(loaded_graph.m_dict_vertices) == self.cnt_vertices, \
            f"No vertices: {len(loaded_graph.m_dict_vertices)}"
        
        # Sort neighbors for each vertex
        for _, vertex_value in loaded_graph.m_dict_vertices.items():
            vertex_value.pNeighbours.sort()
        
        return loaded_graph
    
    def initialize_graph(self, graph: Graph) -> Graph:
        """
        Pre-compute the common neighbors and exclusive neighbors of every
        non-converged edges. Pre-compute sumWeight of every node in G(V,E)
        """
        
        p_edges = graph.get_all_edges()
        cnt_check_sum_weight = 0
        
        
        for edge_key, edge_info in p_edges.items():
            
            vertex_start, vertex_end = Graph.from_key_to_vertex(edge_key)
            
            # Get neighbors of both vertices
            star_u = graph.m_dict_vertices[vertex_start].pNeighbours
            star_v = graph.m_dict_vertices[vertex_end].pNeighbours
            
            # Count common neighbors using two-pointer technique
            i, j = 0, 0
            m, n = len(star_u), len(star_v)
            
            no_common_neighbor = 0
            
            while i < m and j < n:
                a = star_u[i]
                b = star_v[j]
    
                if a == b:
                    no_common_neighbor += 1
                    i += 1
                    j += 1
                elif a > b:
                    j += 1
                else:  # a < b
                    i += 1
            
            # Calculate Jaccard distance
            deg_u = len(star_u) - 1  # Subtract 1 to exclude self
            deg_v = len(star_v) - 1  # Subtract 1 to exclude self
            c = no_common_neighbor
            
            numerator = float(c)  # Common neighbors
            denominator = float(deg_u + deg_v + 2 - c)
            
            # Jaccard distance = 1 - (intersection / union)
            dis = 1.0 - numerator / denominator if denominator != 0 else 1.0
            
            edge_info.distance = dis
            
            graph.update_edge(vertex_start, vertex_end, dis, self.m_i_current_step)
            graph.add_vertex_weight(vertex_start, dis, self.m_i_current_step)
            graph.add_vertex_weight(vertex_end, dis, self.m_i_current_step)
            
            cnt_check_sum_weight += 1
        
        # Debug output if enabled
        if Settings.DEBUG:
            with open("graph_jaccard_initilized", 'w') as distance_init_out:
                for edge_key, edge_info in p_edges.items():
                    
                    distance = edge_info.distance
                    vertex_start, vertex_end = Graph.from_key_to_vertex(edge_key)
                    distance_init_out.write(f"{vertex_start} {vertex_end} {distance:.6f}\n")
                    
        return graph
    
    def init_jaccard(self, graph_file: str):
        """
            Run Jaccard Distance initialization
        """
        tic = time.time()
    
        loaded_graph : Graph = self.setup_graph(graph_file)
        jaccard_initilized_graph = self.initialize_graph(loaded_graph)
        
        toc = time.time()
        running_time = toc - tic
         
        print(f"Jaccard initialization time: {running_time:.3f}\n")
        
        return jaccard_initilized_graph

    
        