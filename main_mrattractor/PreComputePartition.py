#!/usr/bin/env python3
"""
Pre-compute partitions of star graphs. A star graph has a center and list of neighbors. 
We need to find G_{ijk} that a star graph belongs to so that we don't need to re-partition 
the star graph times to times.
"""

import sys
import logging
from typing import List, Tuple, Iterator
import MasterMR
from writable.SpecialEdgeTypeWritable import SpecialEdgeTypeWritable
from writable.StarGraphWithPartitionWritable import StarGraphWithPartitionWritable
from writable.TripleWritable import TripleWritable

class PreComputePartition:
    """
    MapReduce job per pre-computare le partizioni dei grafi stella
    """
    
    JOB_NAME = "PreComputePartition"
    
    @staticmethod
    def node2hash(u: int, no_partitions: int) -> int:
        """Funzione di hash per mappare un nodo alla sua partizione"""
        return u % no_partitions
    
    class Mapper:
        """Mapper class per il job PreComputePartition"""
        
        def __init__(self):
            self.p = 0  # Numero di partizioni disgiunte del grafo, default 20
            self.logger = logging.getLogger(self.__class__.__name__)
        
        def setup(self, context):
            """Setup del mapper"""
            self.p = context.get_configuration().get_int("no_partitions", 0)
        
        def map(self, edge: SpecialEdgeTypeWritable, value, context):
            """
            Funzione di mapping
            
            Args:
                edge: SpecialEdgeTypeWritable - l'edge di input
                value: valore associato (NullWritable)
                context: contesto MapReduce
            """
            try:
                # Verifica che l'input sia un edge
                assert edge.type == 'G', "The input must be an edge"
                
                u = edge.center
                v = edge.target
                hash_u = self.node2hash(u, self.p)
                hash_v = self.node2hash(v, self.p)
                
                if hash_u == hash_v:
                    # Caso: hashU == hashV
                    for a in range(self.p):
                        for b in range(a + 1, self.p):
                            if a == hash_v or b == hash_v:
                                continue
                            
                            triple_graph = TripleWritable()
                            triple_graph.set(a, b, hash_v)
                            
                            # Emetti per entrambi i nodi
                            context.write(u, triple_graph)
                            context.write(v, triple_graph)
                else:
                    # Caso: hashU != hashV
                    for a in range(self.p):
                        if a != hash_u and a != hash_v:
                            triple_graph = TripleWritable()
                            triple_graph.set(a, hash_u, hash_v)
                            
                            # Emetti per entrambi i nodi
                            context.write(u, triple_graph)
                            context.write(v, triple_graph)
                            
            except Exception as e:
                error_msg = f"{MasterMR.prefix_log}{str(e)}"
                self.logger.error(error_msg)
                raise e
        
        def cleanup(self, context):
            """Cleanup del mapper"""
            pass
    
    class Combiner:
        """Combiner per ridurre il traffico di rete"""
        
        def __init__(self):
            self.p = 0
            self.logger = logging.getLogger(self.__class__.__name__)
        
        def setup(self, context):
            """Setup del combiner"""
            self.p = context.get_configuration().get_int("no_partitions", 0)
        
        def reduce(self, vertex_key: int, values: Iterator[TripleWritable], context):
            """
            Funzione di combine per eliminare duplicati
            
            Args:
                vertex_key: chiave del vertice
                values: iteratore di TripleWritable
                context: contesto MapReduce
            """
            try:
                map_triplets = {}
                
                for adj in values:
                    key = str(adj)
                    # Emetti solo se non è già presente nella mappa
                    if key not in map_triplets:
                        context.write(vertex_key, adj)
                        map_triplets[key] = 1
                
                assert len(map_triplets) <= self.p * self.p, \
                    f"Too many triplets: {len(map_triplets)} > {self.p * self.p}"
                    
            except Exception as e:
                error_msg = f"{MasterMR.prefix_log}{str(e)}"
                self.logger.error(error_msg)
                raise e
        
        def cleanup(self, context):
            """Cleanup del combiner"""
            pass
    
    class Reducer:
        """Reducer per aggregare le partizioni"""
        
        def __init__(self):
            self.p = 0
            self.logger = logging.getLogger(self.__class__.__name__)
            self.multiple_outputs = None
        
        def setup(self, context):
            """Setup del reducer"""
            self.p = context.get_configuration().get_int("no_partitions", 0)
            self.multiple_outputs = context.get_multiple_outputs()
        
        def reduce(self, vertex_key: int, values: Iterator[TripleWritable], context):
            """
            Funzione di reduce per creare le partizioni finali
            
            Args:
                vertex_key: chiave del vertice
                values: iteratore di TripleWritable
                context: contesto MapReduce
            """
            try:
                # Debug per vertice specifico
                if vertex_key == 3710:
                    x = 0
                    x += 1
                
                triples = []
                map_triplets = {}
                
                # Raccoglie tutti i triplet unici
                for adj in values:
                    new_adj = TripleWritable()
                    new_adj.set(adj.left, adj.mid, adj.right)
                    map_triplets[new_adj] = 1
                
                assert len(map_triplets) <= self.p * self.p, \
                    f"The number of distinct Triplets should be smaller than p*p: {len(map_triplets)} > {self.p * self.p}"
                
                # Converte in lista
                for triplet in map_triplets.keys():
                    triples.append(triplet)
                
                # Crea l'oggetto SpecialEdgeTypeWritable
                special = SpecialEdgeTypeWritable()
                special.init('S', vertex_key, -1, -1, len(triples), triples, -1, None)
                
                # Scrive l'output usando MultipleOutputs
                self.multiple_outputs.write("graph", special, None, "partitions")
                
            except Exception as e:
                error_msg = f"{MasterMR.prefix_log}{str(e)}"
                self.logger.error(error_msg)
                raise e
        
        def cleanup(self, context):
            """Cleanup del reducer"""
            if self.multiple_outputs:
                self.multiple_outputs.close()
    
    def run(self, args: List[str]) -> int:
        """
        Esegue il job MapReduce
        
        Args:
            args: lista di argomenti [input_path, output_path, no_partitions]
            
        Returns:
            int: codice di ritorno (1 per successo)
        """
        try:
            if len(args) < 3:
                raise ValueError("Usage: PreComputePartition <input_path> <output_path> <no_partitions>")
            
            input_files = args[0]
            output_files = args[1]
            no_partitions = int(args[2])
            
            # Configurazione del job
            job_config = {
                'job_name': self.JOB_NAME,
                'mapper_class': self.Mapper,
                'combiner_class': self.Combiner,
                'reducer_class': self.Reducer,
                'input_format': 'SequenceFileInputFormat',
                'output_format': 'SequenceFileOutputFormat',
                'input_paths': [input_files],
                'output_path': output_files,
                'no_partitions': no_partitions,
                'split_max_size': 5143265,
                'multiple_outputs': {
                    'graph': {
                        'format': 'SequenceFileOutputFormat',
                        'key_class': 'SpecialEdgeTypeWritable',
                        'value_class': 'NullWritable'
                    }
                }
            }
            
            # Qui dovresti integrare con il tuo framework MapReduce Python
            # Ad esempio, se usi mrjob, pyspark, o un altro framework
            
            print(f"Starting job: {self.JOB_NAME}")
            print(f"Input: {input_files}")
            print(f"Output: {output_files}")
            print(f"Partitions: {no_partitions}")
            
            # Placeholder per l'esecuzione del job
            # job_result = execute_mapreduce_job(job_config)
            
            return 1
            
        except Exception as e:
            logging.error(f"Error running job: {str(e)}")
            return 0


# def main():
#     """Funzione main per eseguire il job da command line"""
#     if len(sys.argv) < 4:
#         print("Usage: python precompute_partition.py <input_path> <output_path> <no_partitions>")
#         sys.exit(1)
    
#     job = PreComputePartition()
#     result = job.run(sys.argv[1:])
#     sys.exit(0 if result == 1 else 1)


# if __name__ == "__main__":
#     main()