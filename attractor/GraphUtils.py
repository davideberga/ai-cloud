import time
from libs.Graph import Graph
from libs.Settings import Settings


class GraphUtils:
    """
    First component: computing Jaccard Distance in Single Machine.
    The output of this component will be used for Dynamic Interactions on MapReduce.
    """
    
    def __init__(self):
        """
        Initialize JaccardInit with graph parameters.
        
        Args:
            graph_file: Path to the graph file
        """
        self.m_i_current_step = 0
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
                
                if line.startswith("#"):
                    continue
                
                parts = line.split()
                if len(parts) < 2:
                    continue
                
                try:            
                    vertex_start = int(parts[0])
                    vertex_end = int(parts[1])
                    loaded_graph.add_edge(vertex_start, vertex_end, 0.0)
                except ValueError as e:
                    print(f"Error parsing line {line_num + 1}: {line}")
                    raise ValueError(f"Invalid number format in line {line_num + 1}") from e
        
        for k, v in loaded_graph.m_dict_vertices.items():
            v.neighbours = sorted(v.neighbours)

        return loaded_graph
    
    def initialize_graph(self, graph: Graph) -> Graph:
        
        p_edges = graph.get_all_edges()
        cnt_check_sum_weight = 0
        
        for edge_key, edge_info in p_edges.items():
            
            vertex_start, vertex_end = Graph.from_key_to_vertex(edge_key)
            star_u = graph.m_dict_vertices[vertex_start].neighbours
            star_v = graph.m_dict_vertices[vertex_end].neighbours
            
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
                    
            deg_u = len(star_u) - 1
            deg_v = len(star_v) - 1
            c = no_common_neighbor
            
            numerator = float(c)  
            denominator = float(deg_u + deg_v + 2 - c)
            
            # Jaccard distance = 1 - (intersection / union)
            dis = 1.0 - numerator / denominator
            
            edge_info.weight = dis
            
            graph.update_edge(vertex_start, vertex_end, dis, self.m_i_current_step)
            graph.add_vertex_weight(vertex_start, dis, self.m_i_current_step)
            graph.add_vertex_weight(vertex_end, dis, self.m_i_current_step)
            
            cnt_check_sum_weight += 1
        
        return graph
    
    def init_jaccard(self, graph_file: str):
        """
            Run Jaccard Distance initialization
        """
    
        loaded_graph : Graph = self.setup_graph(graph_file)
        jaccard_initilized_graph = self.initialize_graph(loaded_graph)
        
        return jaccard_initilized_graph
    
    def setup_graph_rdd(self, reduced_edges, loop_counter): #str_filename
        graph = Graph()

        for row in reduced_edges:
            start, end = row[0].split('-')
            start, end = int(start), int(end)
            w = row[1][2]
            graph.add_edge(start, end, w)
            graph.m_dict_edges[row[0]].set_sliding_window(loop_counter, row[1][3])

        return graph
    
    def initialize_graph_rdd(self, graph: Graph) -> Graph:
        
        p_edges = graph.get_all_edges()
        
        for key, edge in p_edges.items():
            
            vertex_start, vertex_end = Graph.from_key_to_vertex(key)
            distance = edge.weight
            
            graph.update_edge(vertex_start, vertex_end, distance, 0)
            graph.add_vertex_weight(vertex_start, distance, 0)
            graph.add_vertex_weight(vertex_end, distance, 0)
            
            if 0 <  edge.weight < 1:
                start_neigh = set(graph.get_vertex_neighbours(vertex_start))
                end_neigh = set(graph.get_vertex_neighbours(vertex_end))
                
                # Compute common neighbours
                comm_n = start_neigh.intersection(end_neigh)
                edge.common_n = list(comm_n)
                
                # Compute exclusive neighbours
                edge.exclusive_n[0] = start_neigh.difference(comm_n)
                edge.exclusive_n[1] = end_neigh.difference(comm_n)
            
                    
        return graph
    
    def init_jaccard_from_rdd(self, reduced_edges, loop_counter):
        """
            Run Jaccard Distance initialization
        """
    
        # loaded_graph  = self.setup_graph()
        loaded_graph = self.setup_graph_rdd(reduced_edges, loop_counter)
        jaccard_initilized_graph = self.initialize_graph_rdd(loaded_graph)
        
        return jaccard_initilized_graph

    
        