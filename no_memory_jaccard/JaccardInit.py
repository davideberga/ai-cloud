import time
import os
from typing import Dict, List, Tuple
from no_memory.Graph import Graph
from no_memory.Helper import Helper
from no_memory.Settings import Settings


class JaccardInit:
    """
    First component: computing Jaccard Distance in Single Machine.
    The output of this component will be used for Dynamic Interactions on MapReduce.
    """
    
    def __init__(self, graph_file: str, num_vertices: int, num_edges: int, lambda_val: float):
        """
        Initialize JaccardInit with graph parameters.
        
        Args:
            graph_file: Path to the graph file
            num_vertices: Number of vertices in the graph
            num_edges: Number of edges in the graph
            lambda_val: Lambda parameter for calculations
        """
        self.m_c_graph = None
        self.m_i_current_step = 0
        self.cnt_vertices = num_vertices
        self.cnt_edges = num_edges
        self.current_loops = 0
        self.log_single = None
        self.graph_file = graph_file
        Settings.lambda_val = lambda_val
    
    def setup_graph(self, str_file_name: str):
        """
        Adding edges to the graph, build dictionary of edges and dictionary of vertices.
        
        Args:
            str_file_name: Path to the graph file
            
        Raises:
            ValueError: If number format is invalid
            IOError: If file cannot be read
        """
        self.m_c_graph = Graph()
        
        try:
            with open(str_file_name, 'r') as file:
                for line_num, line in enumerate(file):
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    
                    try:
                        i_begin = int(parts[0])
                        i_end = int(parts[1])
                        # Distance of the edge (initially 0)
                        d_weight = 0.0
                        self.m_c_graph.add_edge(i_begin, i_end, d_weight)
                    except ValueError as e:
                        print(f"Error parsing line {line_num + 1}: {line}")
                        raise ValueError(f"Invalid number format in line {line_num + 1}") from e
        
        except FileNotFoundError:
            raise IOError(f"File not found: {str_file_name}")
        except Exception as e:
            raise IOError(f"Error reading file {str_file_name}: {str(e)}")
        
        # Assertions to verify graph construction
        assert len(self.m_c_graph.m_dict_edges) == self.cnt_edges, \
            f"No Edges: {len(self.m_c_graph.m_dict_edges)}"
        assert len(self.m_c_graph.m_dict_vertices) == self.cnt_vertices, \
            f"No vertices: {len(self.m_c_graph.m_dict_vertices)}"
        
        # Sort neighbors for each vertex
        for vertex_id, vertex_value in self.m_c_graph.m_dict_vertices.items():
            vertex_value.pNeighbours.sort()
    
    def initialize_graph(self):
        """
        Pre-compute the common neighbors and exclusive neighbors of every
        non-converged edges. Pre-compute sumWeight of every node in G(V,E)
        
        Raises:
            Exception: If graph initialization fails
        """
        p_edges = self.m_c_graph.get_all_edges()
        
        cnt_check_sum_weight = 0
        self.log_single.write("Jaccard Distance Initialization \n")
        self.log_single.flush()
        
        for edge_key, p_edge_value in p_edges.items():
            parts = edge_key.split()
            i_begin = int(parts[0])
            i_end = int(parts[1])
            
            # Get neighbors of both vertices
            star_u = self.m_c_graph.m_dict_vertices[i_begin].pNeighbours
            star_v = self.m_c_graph.m_dict_vertices[i_end].pNeighbours
            
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
            if denominator != 0:
                dis = 1.0 - numerator / denominator
            else:
                dis = 1.0  # Maximum distance if no union
            
            p_edge_value.distance = dis
            
            # Update edge and vertex weights
            d_distance = p_edge_value.distance
            self.m_c_graph.update_edge(i_begin, i_end, d_distance, self.m_i_current_step)
            self.m_c_graph.add_vertex_weight(i_begin, d_distance, self.m_i_current_step)
            self.m_c_graph.add_vertex_weight(i_end, d_distance, self.m_i_current_step)
            
            cnt_check_sum_weight += 1
        
        self.log_single.write("Done finding common neighbors and exclusive neighbors \n")
        self.log_single.flush()
        
        assert cnt_check_sum_weight == self.cnt_edges
        
        # Debug output if enabled
        if Settings.DEBUG:
            with open("distance_init_out", 'w') as distance_init_out:
                for edge_key, p_edge_value in p_edges.items():
                    distance = p_edge_value.distance
                    parts = edge_key.split()
                    i_begin = int(parts[0])
                    i_end = int(parts[1])
                    
                    distance_init_out.write(f"{i_begin} {i_end} {distance:.6f}\n")
    
    def execute(self):
        """
        Run Jaccard Distance initialization on single machine
        
        Raises:
            Exception: If execution fails
        """
        tic = time.time()
        
        # Extract graph name from file path
        graph_name = os.path.basename(self.graph_file)
        log_file_name = f"log_single_attractor_full_{graph_name}.log"
        
        with open(log_file_name, 'w') as self.log_single:
            self.setup_graph(self.graph_file)
            self.initialize_graph()
            
            toc = time.time()
            running_time = toc - tic
            
            self.log_single.write(
                f"Running time of single machine Jaccard Distance Init Attractor is: {running_time:.3f}\n"
            )
        
        # Close debug logs if enabled
        if Settings.DEBUG:
            # Settings.log_each_iteration.close()
            pass