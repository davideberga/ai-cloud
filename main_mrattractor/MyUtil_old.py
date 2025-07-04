import os
import shutil
import pickle
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from no_memory_jaccard.JaccardInit import JaccardInit
import no_memory.EdgeValue_v2 as EdgeValue_v2, no_memory.VertexValue_v2 as VertexValue_v2
import writable.SpecialEdgeTypeWritable as SpecialEdgeTypeWritable

class Settings:
    EDGE_TYPE = "EDGE"

# class EdgeValue_v2:
#     def __init__(self, distance: float = 0.0):
#         self.distance = distance

# class VertexValue_v2:
#     def __init__(self):
#         self.pNeighbours = []

# class SpecialEdgeTypeWritable:
#     def __init__(self):
#         self.edge_type = None
#         self.begin = None
#         self.end = None
#         self.distance = None
        
#     def init(self, edge_type: str, begin: int, end: int, distance: float, 
#              param1: int, param2: Any, param3: int, param4: Any):
#         self.edge_type = edge_type
#         self.begin = begin
#         self.end = end
#         self.distance = distance
        
#     def to_string_for_local_machine(self) -> str:
#         return f"{self.begin} {self.end} {self.distance}"

# class Graph:
#     def __init__(self):
#         self.m_dictVertices: Dict[int, VertexValue_v2] = {}
#         self.edges: Dict[str, EdgeValue_v2] = {}
    
#     def GetAllEdges(self) -> Dict[str, EdgeValue_v2]:
#         return self.edges

# class JaccardInit:
#     def __init__(self, graph_file: str, no_vertices: int, no_edges: int, lambda_val: float):
#         self.graph_file = graph_file
#         self.no_vertices = no_vertices
#         self.no_edges = no_edges
#         self.lambda_val = lambda_val
#         self.m_cGraph = Graph()
    
#     def Execute(self):
#         # Implementazione placeholder - dovrebbe essere sostituita con la logica reale
#         pass

class MyUtil:
    
    @staticmethod
    def merge_sequence_files_to_local(from_directory: str, to_file: str, 
                                    local_fs_path: str = None) -> None:
        """
        Merge dei file sequenziali da una directory a un file locale.
        In Python usiamo pickle per la serializzazione binaria.
        """
        from_path = Path(from_directory)
        
        if not from_path.is_dir():
            raise ValueError(f"'{from_directory}' non è una directory")
        
        merged_data = []
        
        for file_path in from_path.iterdir():
            if file_path.is_dir():
                print(f"Salto la directory {file_path.name}")
                continue
                
            if file_path.name.startswith("_"):
                print(f"Salto il file '_' '{file_path.name}'")
                continue
                
            print(f"Unendo '{file_path.name}'")
            
            try:
                with open(file_path, 'rb') as f:
                    file_data = pickle.load(f)
                    if isinstance(file_data, list):
                        merged_data.extend(file_data)
                    else:
                        merged_data.append(file_data)
            except Exception as e:
                print(f"Errore nel leggere {file_path}: {e}")
        
        # Salva i dati uniti
        with open(to_file, 'wb') as f:
            pickle.dump(merged_data, f)
    
    @staticmethod
    def convert_binary_file_to_text_file(binary_file: str, output_text_file: str) -> None:
        """
        Converte un file binario in un file di testo normale per l'elaborazione su singola macchina.
        """
        try:
            with open(binary_file, 'rb') as bf:
                data = pickle.load(bf)
            
            with open(output_text_file, 'w') as tf:
                if isinstance(data, list):
                    for item in data:
                        if hasattr(item, 'to_string_for_local_machine'):
                            tf.write(item.to_string_for_local_machine() + '\n')
                        else:
                            tf.write(str(item) + '\n')
                else:
                    if hasattr(data, 'to_string_for_local_machine'):
                        tf.write(data.to_string_for_local_machine() + '\n')
                    else:
                        tf.write(str(data) + '\n')
            
            # Elimina il file binario
            os.remove(binary_file)
            
        except Exception as e:
            print(f"Errore nella conversione: {e}")
            raise
    
    @staticmethod
    def merge_files(root_folder: str, merged_file: str) -> None:
        """
        Unisce tutti i file in una cartella in un singolo file.
        """
        try:
            # Rimuove il file di destinazione se esiste
            if os.path.exists(merged_file):
                os.remove(merged_file)
            
            root_path = Path(root_folder)
            
            with open(merged_file, 'wb') as outfile:
                for file_path in root_path.rglob('*'):
                    if file_path.is_file() and not file_path.name.startswith('.'):
                        with open(file_path, 'rb') as infile:
                            shutil.copyfileobj(infile, outfile)
                            
        except Exception as e:
            print(f"Errore nell'unire i file: {e}")
    
    @staticmethod
    def copy_folder(source_folder: str, dest_folder: str) -> None:
        """
        Copia una cartella da sorgente a destinazione.
        """
        try:
            if os.path.exists(dest_folder):
                shutil.rmtree(dest_folder)
            shutil.copytree(source_folder, dest_folder)
        except Exception as e:
            print(f"Errore nella copia della cartella: {e}")
    
    @staticmethod
    def merge_and_convert_binary_files(binary_folder: str, output_text_file_local: str) -> None:
        """
        Unisce e converte tutti i file binari in una cartella in un singolo file di testo.
        """
        folder_path = Path(binary_folder)
        
        if not folder_path.is_dir():
            raise ValueError(f"{binary_folder} non è una directory")
        
        try:
            with open(output_text_file_local, 'w') as writer:
                for file_path in folder_path.iterdir():
                    if file_path.is_file() and not file_path.name.startswith('.'):
                        try:
                            with open(file_path, 'rb') as f:
                                data = pickle.load(f)
                                
                            if isinstance(data, list):
                                for item in data:
                                    if hasattr(item, 'to_string_for_local_machine'):
                                        writer.write(item.to_string_for_local_machine() + '\n')
                            else:
                                if hasattr(data, 'to_string_for_local_machine'):
                                    writer.write(data.to_string_for_local_machine() + '\n')
                                    
                        except Exception as e:
                            print(f"Errore nel processare {file_path}: {e}")
            
            # Elimina la cartella dei file binari
            shutil.rmtree(binary_folder)
            
        except Exception as e:
            print(f"Errore nell'unire e convertire: {e}")
            raise
    
    @staticmethod
    def compute_jaccard_distance_in_single_machine(graph_file: str, binary_graph_file: str,
                                                 no_vertices: int, no_edges: int, 
                                                 lambda_val: float, degfile_out: str) -> None:
        """
        Calcola la distanza di Jaccard su una singola macchina.
        """
        try:
            # Copia il file localmente (simulazione)
            local_graph_file = os.path.basename(graph_file)
            shutil.copy2(graph_file, local_graph_file)
            
            # Usando il file scaricato per il calcolo su singola macchina
            single_attractor = JaccardInit(local_graph_file, no_vertices, no_edges, lambda_val)
            single_attractor.Execute()
            
            local_binary_graph_file = f"local_binary_graph_file_{local_graph_file}"
            
            # Serializza i dati degli archi
            edges_data = []
            all_edges = single_attractor.m_cGraph.GetAllEdges()
            
            for edge_key, edge_value in all_edges.items():
                parts = edge_key.split()
                begin = int(parts[0])
                end = int(parts[1])
                
                spec = SpecialEdgeTypeWritable()
                spec.init(Settings.EDGE_TYPE, begin, end, edge_value.distance, -1, None, -1, None)
                edges_data.append(spec)
            
            # Salva in formato binario
            with open(local_binary_graph_file, 'wb') as f:
                pickle.dump(edges_data, f)
            
            # Copia il file binario alla destinazione finale
            shutil.copy2(local_binary_graph_file, binary_graph_file)
            
            # Rimuove i file temporanei
            os.remove(local_graph_file)
            os.remove(local_binary_graph_file)
            
            # Scrive il file dei gradi
            with open("degfile", 'w') as writer:
                map_vertices = single_attractor.m_cGraph.m_dictVertices
                for vertex_id, vertex_value in map_vertices.items():
                    deg = len(vertex_value.pNeighbours) - 1
                    writer.write(f"{vertex_id} {deg}\n")
            
            # Copia il file dei gradi
            shutil.copy2("degfile", degfile_out)
            os.remove("degfile")
            
        except Exception as e:
            print(f"Errore nel calcolo Jaccard: {e}")
            raise
    
    @staticmethod
    def convert_original_graph_in_sequence_files(graph_file: str, binary_graph_file: str) -> None:
        """
        Converte un file di grafo testuale in file sequenziali binari.
        """
        try:
            # Copia il file localmente
            local_graph_file = os.path.basename(graph_file)
            shutil.copy2(graph_file, local_graph_file)
            
            local_binary_graph_file = f"local_binary_graph_file_{local_graph_file}"
            edges_data = []
            
            # Legge il file di testo del grafo
            with open(local_graph_file, 'r') as reader:
                for line in reader:
                    line = line.strip()
                    if line:
                        args = line.split()
                        u = int(args[0])
                        v = int(args[1])
                        
                        spec = SpecialEdgeTypeWritable()
                        spec.init(Settings.EDGE_TYPE, u, v, 0, -1, None, -1, None)
                        edges_data.append(spec)
            
            # Salva in formato binario
            with open(local_binary_graph_file, 'wb') as f:
                pickle.dump(edges_data, f)
            
            # Copia il file binario alla destinazione finale
            shutil.copy2(local_binary_graph_file, binary_graph_file)
            
            # Rimuove i file temporanei
            os.remove(local_graph_file)
            os.remove(local_binary_graph_file)
            
        except Exception as e:
            print(f"Errore nella conversione del grafo: {e}")
            raise


# # Esempio di utilizzo
# if __name__ == "__main__":
#     # Esempio di come usare la classe
#     util = MyUtil()
    
#     # Esempio: unire file sequenziali
#     # util.merge_sequence_files_to_local("input_directory", "output_file.pkl")
    
#     # Esempio: convertire file binario in testo
#     # util.convert_binary_file_to_text_file("binary_file.pkl", "output.txt")
    
#     print("Utilità MyUtil tradotta in Python pronta per l'uso!")