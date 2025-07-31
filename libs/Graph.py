from typing import Dict, Tuple
from attractor.DataframeSchemaProvider import DataframeSchemaProvider
from libs.Vertex import Vertex
from libs.Edge import Edge
from libs.Settings import EdgeTypeEnum
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession
from libs.Settings import Settings

class Graph:
    def __init__(self):
        self.m_dict_edges: Dict[str, Edge] = {}  # Dict<String, EdgeInfo>
        self.m_dict_vertices: Dict[str, Vertex] = {}  # HashMap<Integer, VertexValue_v2>
        
        self.BEGIN_POINT = 0
        self.END_POINT = 0

    def get_num_vertex(self) -> int:
        return len(self.m_dict_vertices)
    
    def get_num_edges(self) -> int:
        return len(self.m_dict_edges)
    
    def add_edge(self, v1, v2, weight):
        edge_key = self.refine_edge_key(v1, v2)
        if edge_key in self.m_dict_edges:
            return 
        
        self.m_dict_edges[edge_key] = Edge(v1, v2, weight)
        self.add_vertex(v1, v2)
        self.add_vertex(v2, v1)
        
    def add_vertex(self, v, neighbour):
        if v not in self.m_dict_vertices:
            vl = Vertex()
            vl.neighbours.add(v)
            self.m_dict_vertices[v] = vl
            
        self.m_dict_vertices[v].neighbours.add(neighbour)
        
    def get_vertex_neighbours(self, v):
        if v not in self.m_dict_vertices: raise KeyError("Vertex not found")
        return self.m_dict_vertices[v].neighbours
    
    def update_edge(self, v1, v2, distance, step):
        edge_key = self.refine_edge_key(v1, v2)
        if edge_key not in self.m_dict_edges:
            raise Exception("No Such Edges")
        self.m_dict_edges[edge_key].a_distance[step] = distance
    
    def add_vertex_weight(self, v, distance, step):
        if v not in self.m_dict_vertices:
            raise Exception("Vertex is not exist.")
        
        self.m_dict_vertices[v].aWeightSum[step] += 1 - distance
    
    def distance(self, v1, v2, step):
        if v1 == v2:
            return 0
        edge_key = self.refine_edge_key(v1, v2)
        if edge_key not in self.m_dict_edges:
            print(edge_key)
            raise Exception("No edge")
        return self.m_dict_edges[edge_key].a_distance[step]
    
    def weight(self, v1, v2):
        if v1 == v2:
            return 0.0
        edge_key = self.refine_edge_key(v1, v2)
        if edge_key not in self.m_dict_edges:
            return 0.0
        
        return self.m_dict_edges[edge_key].weight
    
    def get_vertex_weight_sum(self, v1, step):
        if v1 not in self.m_dict_vertices:
            raise Exception("Vertex is not exist.")
        return self.m_dict_vertices[v1].aWeightSum[step]
    
    def clear_vertex_weight(self, i_step):
        for vertex_value in self.m_dict_vertices.values():
            vertex_value.aWeightSum[i_step] = 0
    
    def get_all_edges(self) -> Dict[str, Edge]:
        return self.m_dict_edges
    
    def from_key_to_vertex(edge_key: str):
        vertices = edge_key.split()
            
        vertex_start = int(vertices[0])
        vertex_end = int(vertices[1])
        
        return vertex_start, vertex_end
    
    def get_graph_jaccard_dataframe(self, spark: SparkSession) -> DataFrame:
        
        schema = DataframeSchemaProvider.get_schema_graph_jaccard()
        edges_data = []
        edges = self.get_all_edges()
        
        for edge_key, edge_value in edges.items():
             
            vertex_start, vertex_end = Graph.from_key_to_vertex(edge_key)
            
            edge_record = Row(
                edge_type=str(EdgeTypeEnum.G.value),
                begin_vertex=vertex_start,
                end_vertex=vertex_end,
                weight=edge_value.weight,
            )
            edges_data.append(edge_record)
        
        return spark.createDataFrame(edges_data, schema)
    
    def get_degree_dict(self) -> Dict[int, int]:
        vertices_degree = { }
        map_vertices = self.m_dict_vertices
        
        for vertex_id, vertex_value in map_vertices.items():
            degree = len(vertex_value.neighbours) - 1
            vertices_degree[vertex_id] = degree

        # Debug output if enabled
        if Settings.DEBUG:
            with open("graph_degrees", 'w') as degree_init_out:
                for vertex_id, degree_value in vertices_degree.items():
                    degree_init_out.write(f"{vertex_id} {degree_value}\n")
        return  vertices_degree

    @staticmethod
    def refine_edge_key(i_begin, i_end):
       # Create a unique key for the edge
        if i_begin > i_end:
            return f"{i_begin} {i_end}"
        return f"{i_end} {i_begin}"