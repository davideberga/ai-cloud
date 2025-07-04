import math
import time
from collections import defaultdict
import os
import Graph

class CommunityDetection_v2:
    """
    Community Detection algorithm using dynamic interaction approach.
    Translated from Java implementation.
    """
    
    def __init__(self, graph_file, num_vertices, num_edges, lambda_param):
        """
        Initialize the community detection algorithm.
        
        Args:
            graph_file: Path to the graph file
            num_vertices: Number of vertices in the graph
            num_edges: Number of edges in the graph
            lambda_param: Lambda parameter for the algorithm
        """
        self.graph_file = graph_file
        self.cnt_vertices = num_vertices
        self.cnt_edges = num_edges
        self.current_step = 0
        self.current_loops = 0
        self.log_single = None
        self.graph = None
        
        # Assuming Settings and Helper are imported/defined elsewhere
        Settings.lambda_param = lambda_param
    
    def setup_graph(self, filename):
        """
        Build the graph by reading edges from file and creating dictionaries
        of edges and vertices.
        
        Args:
            filename: Path to the graph file
        """
        self.graph = Graph()
        
        with open(filename, 'r') as reader:
            for line in reader:
                line = line.strip()
                if line:
                    parts = line.split()
                    begin = int(parts[0])
                    end = int(parts[1])
                    # Distance of the edge - set to 0 as in original
                    weight = 0.0
                    self.graph.add_edge(begin, end, weight)
        
        # Assertions equivalent to Java Assert.assertTrue
        assert len(self.graph.edges) == self.cnt_edges, f"No Edges: {len(self.graph.edges)}"
        assert len(self.graph.vertices) == self.cnt_vertices, f"No vertices: {len(self.graph.vertices)}"
        
        # Sort neighbors for each vertex
        for vertex_id, vertex_value in self.graph.vertices.items():
            vertex_value.neighbours.sort()
    
    def initialize_graph(self):
        """
        Pre-compute common neighbors and exclusive neighbors of every
        non-converged edge. Pre-compute sum_weight of every node.
        """
        edges = self.graph.get_all_edges()
        cnt_check_sum_weight = 0
        
        for edge_key, edge_value in edges.items():
            parts = edge_key.split()
            begin = int(parts[0])
            end = int(parts[1])
            
            star_u = self.graph.vertices[begin].neighbours
            star_v = self.graph.vertices[end].neighbours
            
            # Find common neighbors using two pointers approach
            i, j = 0, 0
            m, n = len(star_u), len(star_v)
            no_common_neighbor = 0
            
            while i < m and j < n:
                a, b = star_u[i], star_v[j]
                if a == b:
                    no_common_neighbor += 1
                    i += 1
                    j += 1
                elif a > b:
                    j += 1
                else:
                    i += 1
            
            # Calculate distance using Jaccard-like similarity
            deg_u = len(star_u) - 1
            deg_v = len(star_v) - 1
            c = no_common_neighbor
            numerator = float(c)
            denominator = float(deg_u + deg_v + 2 - c)
            distance = 1.0 - numerator / denominator
            edge_value.distance = distance
            
            # Update edge and vertex weights
            self.graph.update_edge(begin, end, distance, self.current_step)
            self.graph.add_vertex_weight(begin, distance, self.current_step)
            self.graph.add_vertex_weight(end, distance, self.current_step)
            
            cnt_check_sum_weight += 1
        
        self.log_single.write("Done finding common neighbors and exclusive neighbors\n")
        self.log_single.flush()
        
        assert cnt_check_sum_weight == self.cnt_edges
        
        if Settings.DEBUG:
            with open("distance_init_out", 'w') as distance_init_out:
                for edge_key, edge_value in edges.items():
                    distance = edge_value.distance
                    parts = edge_key.split()
                    begin = int(parts[0])
                    end = int(parts[1])
                    distance_init_out.write(f"{begin} {end} {distance:.6f}\n")
    
    def dynamic_interaction(self):
        """
        Main dynamic interaction loop for community detection.
        """
        continue_loop = True
        edges = self.graph.get_all_edges()
        
        if Settings.DEBUG:
            Settings.log_each_iteration = open("log_distance_each_iteration", 'w')
        
        cnt_loop = self.current_loops
        loop_single = 0
        
        while continue_loop:
            continue_loop = False
            next_step = Helper.next_step(self.current_step)
            print(f"Single Machine Current Loop: {cnt_loop + 1}")
            
            tic = time.time()
            converge_number = 0
            
            if Settings.DEBUG:
                Settings.log_each_iteration.write(f"Loop: {cnt_loop + 1}\n")
            
            for edge_key, edge_value in edges.items():
                parts = edge_key.split()
                begin = int(parts[0])
                end = int(parts[1])
                
                dd_di = dd_ei = dd_ci = 0.0
                
                current_distance = edge_value.a_distance[self.current_step]
                if 0 < current_distance < 1:
                    star_u = self.graph.vertices[begin].neighbours
                    star_v = self.graph.vertices[end].neighbours
                    
                    # Compute dynamic interaction components
                    d_di = self.compute_di(begin, end, edge_value)
                    d_ci = 0.0
                    d_ei = 0.0
                    
                    # Process common and exclusive neighbors
                    i, j = 0, 0
                    m, n = len(star_u), len(star_v)
                    
                    while i < m and j < n:
                        a, b = star_u[i], star_v[j]
                        if a == b:
                            common = star_u[i]
                            d_ci += self.compute_ci2(begin, end, common)
                            i += 1
                            j += 1
                        elif a > b:
                            d_ei += self.compute_ei2(end, begin, star_v[j])
                            j += 1
                        else:
                            d_ei += self.compute_ei2(begin, end, star_u[i])
                            i += 1
                    
                    # Process remaining neighbors
                    while i < m:
                        d_ei += self.compute_ei2(begin, end, star_u[i])
                        i += 1
                    
                    while j < n:
                        d_ei += self.compute_ei2(end, begin, star_v[j])
                        j += 1
                    
                    delta = d_di + d_ci + d_ei
                    dd_di, dd_ei, dd_ci = d_di, d_ei, d_ci
                    
                    if abs(delta) > Settings.PRECISE:
                        new_distance = self.graph.distance(begin, end, self.current_step) + delta
                        
                        # Clamp distance to [0, 1]
                        if new_distance > 1 - Settings.PRECISE:
                            new_distance = 1.0
                        elif new_distance < Settings.PRECISE:
                            new_distance = 0.0
                        
                        self.graph.update_edge(begin, end, new_distance, next_step)
                        self.graph.add_vertex_weight(begin, new_distance, next_step)
                        self.graph.add_vertex_weight(end, new_distance, next_step)
                        continue_loop = True
                else:
                    # Edge has converged
                    edge_value.a_distance[next_step] = edge_value.a_distance[self.current_step]
                    new_distance = edge_value.a_distance[self.current_step]
                    self.graph.add_vertex_weight(begin, new_distance, next_step)
                    self.graph.add_vertex_weight(end, new_distance, next_step)
                    converge_number += 1
                
                if Settings.DEBUG:
                    test_str = (f"{edge_key} ,oldDis: {self.graph.distance(begin, end, self.current_step):.8f}, "
                              f"newDis: {self.graph.distance(begin, end, next_step):.8f}, "
                              f"DI: {dd_di:.8f}, EI: {dd_ei:.8f}, CI: {dd_ci:.8f}")
                    Settings.log_each_iteration.write(test_str + "\n")
            
            toc = time.time()
            cnt_loop += 1
            loop_single += 1
            
            self.log_single.write(f"Current Iteration of single machine: {cnt_loop} "
                                f"Running Time: {toc - tic:.3f}\n")
            self.log_single.flush()
            
            self.graph.clear_vertex_weight(self.current_step)
            self.current_step = Helper.update_step(self.current_step)
        
        self.log_single.write(f"#Loops of single machine: {loop_single - 1}\n")
    
    def compute_di(self, begin, end, edge_value):
        """
        Compute Direct Interaction component.
        """
        current_distance = edge_value.a_distance[self.current_step]
        begin_neighbors = len(self.graph.get_vertex_neighbours(begin)) - 1
        end_neighbors = len(self.graph.get_vertex_neighbours(end)) - 1
        
        return (-math.sin(1 - current_distance) * 
                (1.0 / begin_neighbors + 1.0 / end_neighbors))
    
    def compute_ci2(self, begin, end, common):
        """
        Compute Common Interaction component.
        """
        shared_vertex = common
        
        # Avoid re-computation
        if begin == shared_vertex or end == shared_vertex:
            return 0.0
        
        d_begin = self.graph.distance(begin, shared_vertex, self.current_step)
        d_end = self.graph.distance(end, shared_vertex, self.current_step)
        
        begin_deg = len(self.graph.get_vertex_neighbours(begin)) - 1
        end_deg = len(self.graph.get_vertex_neighbours(end)) - 1
        
        d_ci = (math.sin(1 - d_begin) * (1 - d_end) / begin_deg +
                math.sin(1 - d_end) * (1 - d_begin) / end_deg)
        
        return -d_ci
    
    def compute_ei2(self, begin, end, exclusive_neighbor_of_begin):
        """
        Compute Exclusive Interaction component.
        """
        d_ei = self.compute_partial_ei2(begin, end, exclusive_neighbor_of_begin)
        return -d_ei
    
    def compute_partial_ei2(self, target, target_neighbour, target_en):
        """
        Compute partial Exclusive Interaction.
        """
        distance = (math.sin(1 - self.graph.distance(target_en, target, self.current_step)) *
                   self.compute_influence(target_neighbour, target_en, target) /
                   (len(self.graph.get_vertex_neighbours(target)) - 1))
        
        return distance
    
    def compute_influence(self, target_neighbour, en_vertex, target):
        """
        Compute influence between vertices.
        """
        distance = 1 - self.compute_virtual_distance(target_neighbour, en_vertex, target)
        
        if distance >= Settings.lambda_param:
            return distance
        
        return distance - Settings.lambda_param
    
    def compute_virtual_distance(self, begin, end, target):
        """
        Compute virtual distance between vertices.
        """
        numerator = 0.0
        begin_neighbours = self.graph.get_vertex_neighbours(begin)
        end_neighbours = self.graph.get_vertex_neighbours(end)
        
        # Find common neighbors using two pointers
        i, j = 0, 0
        m, n = len(begin_neighbours), len(end_neighbours)
        
        while i < m and j < n:
            a, b = begin_neighbours[i], end_neighbours[j]
            if a == b:
                vertex = begin_neighbours[i]
                d_begin = self.graph.distance(begin, vertex, self.current_step)
                d_end = self.graph.distance(end, vertex, self.current_step)
                numerator += (1 - d_begin) + (1 - d_end)
                i += 1
                j += 1
            elif a > b:
                j += 1
            else:
                i += 1
        
        denominator = (self.graph.get_vertex_weight_sum(begin, self.current_step) +
                      self.graph.get_vertex_weight_sum(end, self.current_step))
        
        distance = 1 - numerator / denominator
        return distance
    
    def execute(self):
        """
        Execute the complete community detection algorithm.
        """
        tic = time.time()
        
        # Extract graph name from file path
        graph_name = os.path.basename(self.graph_file)
        log_filename = f"log_single_attractor_full_{graph_name}.log"
        
        self.log_single = open(log_filename, 'w')
        
        try:
            self.setup_graph(self.graph_file)
            self.initialize_graph()
            self.dynamic_interaction()
            
            toc = time.time()
            self.log_single.write(f"Running time of single machine Attractor is: {toc - tic:.3f}\n")
            
        finally:
            self.log_single.close()
            
            if Settings.DEBUG and hasattr(Settings, 'log_each_iteration'):
                Settings.log_each_iteration.close()


# These classes/modules would need to be implemented separately:
# - Graph: Main graph data structure
# - VertexValue_v2: Vertex representation
# - EdgeValue_v2: Edge representation  
# - Settings: Configuration and debugging flags
# - Helper: Utility functions for step management

class Settings:
    """Configuration settings for the algorithm."""
    DEBUG = False
    PRECISE = 1e-6
    lambda_param = 0.5
    log_each_iteration = None

class Helper:
    """Helper functions for step management."""
    
    @staticmethod
    def next_step(current_step):
        """Get next step index (typically 1 - current_step for binary buffering)."""
        return 1 - current_step
    
    @staticmethod
    def update_step(current_step):
        """Update to next step."""
        return 1 - current_step