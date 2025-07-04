import struct
from typing import List, BinaryIO
from PairWritable import PairWritable  # Assuming PairWritable is defined in PairWritable.py

class EdgesWritable:
    def __init__(self):
        self.edges: List['PairWritable'] = []
        self.no_edges: int = 0
    
    def set(self, edges: List['PairWritable']):
        """Imposta la lista di edges e aggiorna il contatore"""
        self.edges = edges
        self.no_edges = len(edges)
    
    def read_fields(self, data_input: BinaryIO):
        """Legge i dati da un input binario"""
        # Legge il numero di edges come intero (4 bytes, big-endian)
        no_edges_bytes = data_input.read(4)
        if len(no_edges_bytes) < 4:
            raise IOError("Impossibile leggere il numero di edges")
        
        self.no_edges = struct.unpack('>i', no_edges_bytes)[0]
        self.edges = []
        
        # Legge ogni PairWritable
        for i in range(self.no_edges):
            # Assumendo che PairWritable sia già implementata con read_fields
            p = PairWritable()
            p.read_fields(data_input)
            self.edges.append(p)
    
    def write(self, data_output: BinaryIO):
        """Scrive i dati su un output binario"""
        # Scrive il numero di edges come intero (4 bytes, big-endian)
        data_output.write(struct.pack('>i', self.no_edges))
        
        # Scrive ogni PairWritable
        for i in range(self.no_edges):
            p = self.edges[i]
            p.write(data_output)
    
    def compare_to(self, target: 'EdgesWritable') -> int:
        """Confronta con un altro EdgesWritable"""
        str1 = str(self).lower()
        str2 = str(target).lower()
        
        if str1 < str2:
            return -1
        elif str1 > str2:
            return 1
        else:
            return 0
    
    def __str__(self) -> str:
        """Rappresentazione string dell'oggetto"""
        sb = []
        for i in range(self.no_edges):
            curr = self.edges[i]
            e = f"{curr.left} {curr.right}"
            sb.append(e)
        return ",".join(sb) + ","
    
    def __hash__(self) -> int:
        """Hash dell'oggetto basato sulla rappresentazione string"""
        return hash(str(self))
    
    def __eq__(self, other) -> bool:
        """Uguaglianza basata sulla rappresentazione string"""
        if not isinstance(other, EdgesWritable):
            return False
        return str(self) == str(other)
    
    def __lt__(self, other: 'EdgesWritable') -> bool:
        """Supporto per ordinamento"""
        return self.compare_to(other) < 0