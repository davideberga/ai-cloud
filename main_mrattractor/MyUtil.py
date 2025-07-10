from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import Row
import os, sys
import shutil
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from no_memory_jaccard.JaccardInit import JaccardInit

spark = SparkSession.builder.appName("MyUtilPySpark").getOrCreate()
sc = spark.sparkContext

class MyUtil:

    @staticmethod
    def merge_sequence_files_to_local(hdfs_path, local_output):
        rdd = sc.sequenceFile(hdfs_path, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
        rdd.map(lambda x: f"{x[0].toString()}\t{x[1].toString()}").saveAsTextFile(local_output)

    @staticmethod
    def convert_binary_file_to_text_file(binary_file, output_text_file):
        rdd = sc.sequenceFile(binary_file, "com.mrattractor.writable.SpecialEdgeTypeWritable", "org.apache.hadoop.io.NullWritable")
        rdd.map(lambda x: x[0].toStringForLocalMachine()).saveAsTextFile(output_text_file)
        MyUtil.delete_path(binary_file)

    @staticmethod
    def merge_files(root_folder, merged_file):
        with open(merged_file, 'w') as outfile:
            for fname in os.listdir(root_folder):
                if not fname.startswith("_"):
                    with open(os.path.join(root_folder, fname)) as infile:
                        shutil.copyfileobj(infile, outfile)

    @staticmethod
    def copy_folder(hdfs_folder, local_folder):
        if os.path.exists(local_folder):
            shutil.rmtree(local_folder)
        df = spark.read.text(hdfs_folder)
        df.write.mode("overwrite").text("file://" + os.path.abspath(local_folder))

    @staticmethod
    def merge_and_convert_binary_files(binary_folder, output_text_file_local):
        rdd = sc.sequenceFile(binary_folder, "com.mrattractor.writable.SpecialEdgeTypeWritable", "org.apache.hadoop.io.NullWritable")
        rdd.map(lambda x: x[0].toStringForLocalMachine()).saveAsTextFile(output_text_file_local)
        MyUtil.delete_path(binary_folder)

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
        # finally:
        #     # Chiude la sessione Spark
        #     spark.stop()


    @staticmethod
    def convert_original_graph_to_sequence(hdfs_graph_file, binary_graph_file_hdfs):
        local_file = os.path.basename(hdfs_graph_file)
        spark.read.text(hdfs_graph_file).write.mode("overwrite").text("file://" + local_file)

        from writable import SpecialEdgeTypeWritable, Settings

        with open(local_file, 'r') as f:
            lines = f.readlines()

        output_rdd = sc.parallelize([
            (SpecialEdgeTypeWritable.init(Settings.EDGE_TYPE, int(u), int(v), 0, -1, None, -1, None), None)
            for line in lines if (u := line.split()[0]) and (v := line.split()[1])
        ])
        output_rdd.saveAsSequenceFile(binary_graph_file_hdfs)
        os.remove(local_file)

    @staticmethod
    def delete_path(path: str):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
