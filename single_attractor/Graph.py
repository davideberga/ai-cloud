from single_attractor.EdgeValue import EdgeValue
from single_attractor.VertexValue import VertexValue

class Graph:
    def __init__(self):
        self.m_dict_edges = {}  # HashMap<String, EdgeValue>
        self.m_dict_vertices = {}  # HashMap<Integer, VertexValue>
        self.BEGIN_POINT = 0
        self.END_POINT = 0
    
    def add_edge(self, i_begin, i_end, d_weight):
        """
        Aggiunge un arco al grafo
        
        Args:
            i_begin (int): Vertice di inizio
            i_end (int): Vertice di fine
            d_weight (float): Peso dell'arco
            
        Returns:
            bool: True se l'arco è stato aggiunto, False se già esistente
        """
        edge_key = self.refine_edge_key(i_begin, i_end)
        
        if edge_key in self.m_dict_edges:
            return False
        
        # if d_weight > 0 and d_weight < 1:
        self.m_dict_edges[edge_key] = EdgeValue(d_weight)
        
        self.add_vertex(i_begin, i_end)
        self.add_vertex(i_end, i_begin)
        
        return True
    
    def update_edge(self, i_begin, i_end, d_new_distance, i_step):
        """
        Aggiorna la distanza di un arco per uno step specifico
        
        Args:
            i_begin (int): Vertice di inizio
            i_end (int): Vertice di fine
            d_new_distance (float): Nuova distanza
            i_step (int): Step per cui aggiornare la distanza
            
        Raises:
            Exception: Se l'arco non esiste
        """
        edge_key = self.refine_edge_key(i_begin, i_end)
        
        if edge_key not in self.m_dict_edges:
            raise Exception("No Such Edges")
        
        self.m_dict_edges[edge_key].a_distance[i_step] = d_new_distance
    
    def distance(self, i_begin, i_end, i_step):
        """
        Ottiene la distanza tra due vertici per uno step specifico
        
        Args:
            i_begin (int): Vertice di inizio
            i_end (int): Vertice di fine
            i_step (int): Step specifico
            
        Returns:
            float: La distanza tra i vertici
            
        Raises:
            Exception: Se l'arco non esiste
        """
        if i_begin == i_end:
            return 0
        
        edge_key = self.refine_edge_key(i_begin, i_end)
        
        if edge_key not in self.m_dict_edges:
            raise Exception("No edge")
        
        return self.m_dict_edges[edge_key].a_distance[i_step]
    
    def weight(self, i_begin, i_end):
        """
        Ottiene il peso di un arco
        
        Args:
            i_begin (int): Vertice di inizio
            i_end (int): Vertice di fine
            
        Returns:
            float: Il peso dell'arco, 0.0 se non esiste
        """
        if i_begin == i_end:
            return 0.0
        
        edge_key = self.refine_edge_key(i_begin, i_end)
        
        if edge_key not in self.m_dict_edges:
            return 0.0
        
        return self.m_dict_edges[edge_key].weight
    
    def get_vertex_weight_sum(self, i_vertex_id, i_step):
        """
        Ottiene la somma dei pesi di un vertice per uno step specifico
        
        Args:
            i_vertex_id (int): ID del vertice
            i_step (int): Step specifico
            
        Returns:
            float: La somma dei pesi
            
        Raises:
            Exception: Se il vertice non esiste
        """
        if i_vertex_id not in self.m_dict_vertices:
            raise Exception("Vertex is not exist.")
        
        return self.m_dict_vertices[i_vertex_id].a_weight_sum[i_step]
    
    def add_vertex_weight(self, i_vertex_id, d_distance, i_step):
        """
        Aggiunge peso a un vertice per uno step specifico
        
        Args:
            i_vertex_id (int): ID del vertice
            d_distance (float): Distanza da aggiungere
            i_step (int): Step specifico
            
        Raises:
            Exception: Se il vertice non esiste
        """
        if i_vertex_id not in self.m_dict_vertices:
            raise Exception("Vertex is not exist.")
        
        self.m_dict_vertices[i_vertex_id].a_weight_sum[i_step] += 1 - d_distance
    
    def clear_vertex_weight(self, i_step):
        """
        Azzera i pesi di tutti i vertici per uno step specifico
        
        Args:
            i_step (int): Step specifico
        """
        for vertex_value in self.m_dict_vertices.values():
            vertex_value.a_weight_sum[i_step] = 0
    
    def get_all_edges(self):
        """
        Ottiene tutti gli archi del grafo
        
        Returns:
            dict: Dizionario con tutti gli archi
        """
        return self.m_dict_edges
    
    def get_vertex_neighbours(self, i_vertex_id):
        """
        Ottiene i vicini di un vertice
        
        Args:
            i_vertex_id (int): ID del vertice
            
        Returns:
            set: Set contenente gli ID dei vertici vicini
            
        Raises:
            Exception: Se il vertice non esiste
        """
        if i_vertex_id not in self.m_dict_vertices:
            raise Exception("No such an iVertexId.")
        
        vertex_value = self.m_dict_vertices[i_vertex_id]
        return vertex_value.p_neighbours
    
    def add_vertex(self, i_begin, i_end):
        """
        Aggiunge un vertice e la sua connessione
        
        Args:
            i_begin (int): Vertice di partenza
            i_end (int): Vertice di destinazione
        """
        if i_begin not in self.m_dict_vertices:
            vl = VertexValue()
            self.m_dict_vertices[i_begin] = vl
            vl.p_neighbours.add(i_begin)
        
        self.m_dict_vertices[i_begin].p_neighbours.add(i_end)
    
    @staticmethod
    def refine_edge_key(i_begin, i_end):
        """
        Crea una chiave standardizzata per l'arco
        
        Args:
            i_begin (int): Vertice di inizio
            i_end (int): Vertice di fine
            
        Returns:
            str: Chiave dell'arco standardizzata
        """
        if i_begin > i_end:
            return f"{i_begin} {i_end}"
        return f"{i_end} {i_begin}"