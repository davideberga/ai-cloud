from pyspark.sql import SparkSession
import os, sys
import shutil
from no_memory_jaccard.JaccardInit import JaccardInit


class MyUtil:

    @staticmethod
    def merge_files(root_folder, merged_file):
        with open(merged_file, 'w') as outfile:
            for fname in os.listdir(root_folder):
                if not fname.startswith("_"):
                    with open(os.path.join(root_folder, fname)) as infile:
                        shutil.copyfileobj(infile, outfile)

    # @staticmethod
    # def copy_folder(hdfs_folder, local_folder):
    #     if os.path.exists(local_folder):
    #         shutil.rmtree(local_folder)
    #     df = spark.read.text(hdfs_folder)
    #     df.write.mode("overwrite").text("file://" + os.path.abspath(local_folder))

    @staticmethod
    def compute_jaccard_distance_single_machine(graph_file, binary_graph_file, no_vertices, no_edges, lambda_val, degfile_out):
        
        try:
            # Esegue il calcolo della distanza di Jaccard
            single_attractor = JaccardInit(graph_file, no_vertices, no_edges, lambda_val)
            single_attractor.execute()
                
            print("Calcolo della distanza di Jaccard completato con successo!")

            return single_attractor
            
        except Exception as e:
            print(f"Errore durante il calcolo: {str(e)}")
            raise


    # @staticmethod
    # def convert_original_graph_to_sequence(hdfs_graph_file, binary_graph_file_hdfs):
    #     local_file = os.path.basename(hdfs_graph_file)
    #     spark.read.text(hdfs_graph_file).write.mode("overwrite").text("file://" + local_file)

    #     from writable import SpecialEdgeTypeWritable, Settings

    #     with open(local_file, 'r') as f:
    #         lines = f.readlines()

    #     output_rdd = sc.parallelize([
    #         (SpecialEdgeTypeWritable.init(Settings.EDGE_TYPE, int(u), int(v), 0, -1, None, -1, None), None)
    #         for line in lines if (u := line.split()[0]) and (v := line.split()[1])
    #     ])
    #     output_rdd.saveAsSequenceFile(binary_graph_file_hdfs)
    #     os.remove(local_file)

    @staticmethod
    def delete_path(path: str):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
