from bitarray import bitarray  # serve installare 'bitarray' con pip install bitarray
from typing import List, Optional
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from writable.Settings import Settings # Assicurati che questo modulo esista e sia corretto 
from writable.TripleWritable import TripleWritable  # Assicurati che questo modulo esista e sia corretto

class SpecialEdgeTypeWritable:
    def __init__(self):
        self.type: Optional[str] = None
        self.center: Optional[int] = None
        self.target: Optional[int] = None
        self.weight: Optional[float] = None

        self.no_triplet_graphs: Optional[int] = None
        self.triplet_graphs: Optional[List['TripleWritable']] = None

        self.noBits: Optional[int] = None
        self.sliding: Optional[bitarray] = None

    def init(self, _type, _center, _target=None, _weight=None,
             _no_triplet_graphs=None, _triples=None,
             _noBits=None, _sliding=None):
        self.type = _type
        if self.type == Settings.EDGE_TYPE:
            self.center = _center
            self.target = _target
            self.weight = _weight
        elif self.type == Settings.STAR_GRAPH:
            self.center = _center
            self.no_triplet_graphs = _no_triplet_graphs
            self.triplet_graphs = _triples
            assert self.triplet_graphs is not None
            assert len(self.triplet_graphs) == self.no_triplet_graphs
        elif self.type in (Settings.C_TYPE, Settings.D_TYPE, Settings.E_TYPE, Settings.INTERACTION_TYPE):
            self.center = _center
            self.target = _target
            self.weight = _weight
        elif self.type == Settings.SLIDING:
            self.center = _center
            self.target = _target
            self.noBits = _noBits
            self.sliding = _sliding
        else:
            # tipo non riconosciuto, puoi lanciare eccezione o lasciare vuoto
            pass

    def read_fields(self, data_input):
        """Simula la lettura da stream binario (data_input deve essere un file-like oggetto)"""
        self.type = data_input.read(1).decode('utf-8')  # legge un carattere
        if self.type == Settings.EDGE_TYPE:
            self.center = int.from_bytes(data_input.read(4), 'big')
            self.target = int.from_bytes(data_input.read(4), 'big')
            self.weight = float.fromhex(data_input.read(8).hex())  # semplificato, serve adattare
        elif self.type == Settings.STAR_GRAPH:
            self.center = int.from_bytes(data_input.read(4), 'big')
            self.no_triplet_graphs = int.from_bytes(data_input.read(4), 'big')
            self.triplet_graphs = []
            for _ in range(self.no_triplet_graphs):
                t = TripleWritable()
                t.read_fields(data_input)
                self.triplet_graphs.append(t)
        elif self.type in (Settings.C_TYPE, Settings.D_TYPE, Settings.E_TYPE, Settings.INTERACTION_TYPE):
            self.center = int.from_bytes(data_input.read(4), 'big')
            self.target = int.from_bytes(data_input.read(4), 'big')
            self.weight = float.fromhex(data_input.read(8).hex())  # da adattare
        elif self.type == Settings.SLIDING:
            self.center = int.from_bytes(data_input.read(4), 'big')
            self.target = int.from_bytes(data_input.read(4), 'big')
            self.noBits = int.from_bytes(data_input.read(4), 'big')
            bits = []
            for _ in range(self.noBits):
                b = data_input.read(1)
                bits.append(b != b'\x00')
            self.sliding = bitarray(bits)
        else:
            pass

    def write(self, data_output):
        """Simula la scrittura su stream binario (data_output file-like)"""
        data_output.write(self.type.encode('utf-8'))
        if self.type == Settings.EDGE_TYPE:
            data_output.write(self.center.to_bytes(4, 'big'))
            data_output.write(self.target.to_bytes(4, 'big'))
            # scrittura double va adattata: ad es. struct.pack
        elif self.type == Settings.STAR_GRAPH:
            data_output.write(self.center.to_bytes(4, 'big'))
            data_output.write(self.no_triplet_graphs.to_bytes(4, 'big'))
            for triplet in self.triplet_graphs:
                triplet.write(data_output)
        elif self.type in (Settings.C_TYPE, Settings.D_TYPE, Settings.E_TYPE, Settings.INTERACTION_TYPE):
            data_output.write(self.center.to_bytes(4, 'big'))
            data_output.write(self.target.to_bytes(4, 'big'))
            # scrittura double
        elif self.type == Settings.SLIDING:
            data_output.write(self.center.to_bytes(4, 'big'))
            data_output.write(self.target.to_bytes(4, 'big'))
            data_output.write(self.noBits.to_bytes(4, 'big'))
            for i in range(self.noBits):
                data_output.write(b'\x01' if self.sliding[i] else b'\x00')
        else:
            pass

    def to_string_for_local_machine(self):
        if self.type == Settings.EDGE_TYPE:
            return f"{self.center} {self.target} {self.weight} {self.type}"
        elif self.type == Settings.STAR_GRAPH:
            raise Exception("This is not type for single machine")
        elif self.type in (Settings.D_TYPE, Settings.C_TYPE, Settings.E_TYPE, Settings.INTERACTION_TYPE):
            raise Exception("This is not type for single machine")
        elif self.type == Settings.SLIDING:
            bits_str = ' '.join('1' if b else '0' for b in self.sliding)
            return f"{self.center} {self.target} {self.type} {bits_str}"
        else:
            return str(self)

    def __str__(self):
        if self.type == Settings.EDGE_TYPE:
            return f"{self.type} {self.center} {self.target} {self.weight}"
        elif self.type == Settings.STAR_GRAPH:
            trip_str = ",".join(str(trip) for trip in self.triplet_graphs)
            return f"{self.type} {self.center} {trip_str}"
        elif self.type in (Settings.D_TYPE, Settings.C_TYPE, Settings.E_TYPE, Settings.INTERACTION_TYPE):
            return f"{self.type} {self.center} {self.target} {self.weight}"
        elif self.type == Settings.SLIDING:
            bits_str = ' '.join('1' if b else '0' for b in self.sliding)
            return f"{self.type} {self.center} {self.target} {bits_str}"
        else:
            return super().__str__()
