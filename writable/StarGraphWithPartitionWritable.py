import struct
from typing import List, Optional
from io import BytesIO

# Assumendo che queste classi siano già implementate in altri file
from writable.NeighborWritable import NeighborWritable
from writable.TripleWritable import TripleWritable


class StarGraphWithPartitionWritable:
    """
    Traduzione Python della classe Java StarGraphWithPartitionWritable
    che implementa l'interfaccia Writable di Hadoop
    """
    
    def __init__(self):
        # Campi pubblici
        self.center: int = 0
        self.deg_center: int = 0  # degree of center node
        self.neighbors: Optional[List[NeighborWritable]] = None
        
        self.no_triple_sub_graphs: int = 0
        self.triple_sub_graphs: Optional[List[TripleWritable]] = None
    
    def set(self, center: int, 
            neighbors: Optional[List[NeighborWritable]] = None, 
            triple_sub_graphs: Optional[List[TripleWritable]] = None):
        """
        Imposta i valori dell'oggetto StarGraph
        
        Args:
            center: ID del nodo centrale
            neighbors: Lista dei vicini del nodo centrale
            triple_sub_graphs: Lista dei sottografi tripli
        """
        self.center = center
        
        # Star graph
        if neighbors is None:
            self.deg_center = -1
            self.neighbors = None
        else:
            self.deg_center = len(neighbors)
            self.neighbors = neighbors
        
        # Triplet partitions
        if triple_sub_graphs is None:
            self.no_triple_sub_graphs = -1
            self.triple_sub_graphs = None
        else:
            self.no_triple_sub_graphs = len(triple_sub_graphs)
            self.triple_sub_graphs = triple_sub_graphs
    
    def read_fields(self, data_input: BytesIO):
        """
        Legge i campi da un DataInput (equivalente a readFields in Java)
        
        Args:
            data_input: Stream di dati da cui leggere
        """
        # Legge gli interi usando big-endian (formato Java)
        self.center = struct.unpack('>i', data_input.read(4))[0]
        self.deg_center = struct.unpack('>i', data_input.read(4))[0]
        self.no_triple_sub_graphs = struct.unpack('>i', data_input.read(4))[0]
        
        # Legge i neighbors se presenti
        if self.deg_center > 0:
            self.neighbors = []
            for i in range(self.deg_center):
                neighbor = NeighborWritable()
                neighbor.read_fields(data_input)
                self.neighbors.append(neighbor)
        
        # Legge i triple sub graphs se presenti
        if self.no_triple_sub_graphs > 0:
            self.triple_sub_graphs = []
            for i in range(self.no_triple_sub_graphs):
                subgraph = TripleWritable()
                subgraph.read_fields(data_input)
                self.triple_sub_graphs.append(subgraph)
    
    def write(self, data_output: BytesIO):
        """
        Scrive i campi su un DataOutput (equivalente a write in Java)
        
        Args:
            data_output: Stream di dati su cui scrivere
        """
        # Scrive gli interi usando big-endian (formato Java)
        data_output.write(struct.pack('>i', self.center))
        data_output.write(struct.pack('>i', self.deg_center))
        data_output.write(struct.pack('>i', self.no_triple_sub_graphs))
        
        # Scrive i neighbors se presenti
        if self.deg_center != -1 and self.neighbors is not None:
            for neighbor in self.neighbors:
                neighbor.write(data_output)
        
        # Scrive i triple sub graphs se presenti
        if self.no_triple_sub_graphs != -1 and self.triple_sub_graphs is not None:
            for subgraph in self.triple_sub_graphs:
                subgraph.write(data_output)
    
    def __hash__(self):
        """
        Implementazione del metodo hashCode di Java
        """
        # Implementazione base, può essere migliorata se necessario
        return hash((self.center, self.deg_center, self.no_triple_sub_graphs))
    
    def __str__(self):
        """
        Implementazione del metodo toString di Java
        """
        parts = [str(self.center), str(self.deg_center), str(self.no_triple_sub_graphs)]
        
        # Aggiunge i neighbors se presenti
        if self.deg_center > 0 and self.neighbors is not None:
            for neighbor in self.neighbors:
                parts.append("," + str(neighbor))
        
        result = " ".join(parts) + ";"
        
        # Aggiunge i triple sub graphs se presenti
        if self.no_triple_sub_graphs > 0 and self.triple_sub_graphs is not None:
            for subgraph in self.triple_sub_graphs:
                result += str(subgraph) + ";"
        
        return result
    
    def __repr__(self):
        """
        Rappresentazione più dettagliata per il debugging
        """
        return (f"StarGraphWithPartitionWritable(center={self.center}, "
                f"deg_center={self.deg_center}, "
                f"no_triple_sub_graphs={self.no_triple_sub_graphs}, "
                f"neighbors={self.neighbors}, "
                f"triple_sub_graphs={self.triple_sub_graphs})")