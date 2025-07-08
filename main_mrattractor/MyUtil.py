from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import Row
import os, sys
import shutil
import tempfile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from no_memory_jaccard.JaccardInit import JaccardInit
from writable.Settings import Settings
from pyspark.sql.functions import concat_ws

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
        # Inizializza SparkSession
        spark = SparkSession.builder \
            .appName("JaccardDistanceComputation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        try:
            # 1. Prepara il file del grafo
            downloaded_graph_file = os.path.basename(graph_file)
            local_temp_dir = tempfile.mkdtemp()
            local_graph_path = os.path.join(local_temp_dir, downloaded_graph_file)
            
            print(f"hdfs_graph_file: {graph_file}")
            print(f"downloaded_graph_file: {downloaded_graph_file}")
            
            # 2. Esegue il calcolo della distanza di Jaccard
            single_attractor = JaccardInit(graph_file, no_vertices, no_edges, lambda_val)
            single_attractor.execute()
            
            # 3. Prepara i dati per il salvataggio in formato Spark
            edges_data = []
            p_edges = single_attractor.m_c_graph.get_all_edges()
            
            for edge_key, edge_value in p_edges.items():
                parts = edge_key.split()
                i_begin = int(parts[0])
                i_end = int(parts[1])
                
                # Crea un record per Spark DataFrame
                edge_record = Row(
                    edge_type=Settings.EDGE_TYPE,
                    begin_vertex=i_begin,
                    end_vertex=i_end,
                    distance=edge_value.distance,
                    additional_field_1=-1,
                    additional_field_2=None,
                    additional_field_3=-1,
                    additional_field_4=None
                )
                edges_data.append(edge_record)
            
            # 4. Crea DataFrame Spark 
            schema = StructType([
                StructField("edge_type", StringType(), True),
                StructField("begin_vertex", IntegerType(), True),
                StructField("end_vertex", IntegerType(), True),
                StructField("distance", DoubleType(), True),
                StructField("additional_field_1", IntegerType(), True),
                StructField("additional_field_2", StringType(), True),
                StructField("additional_field_3", IntegerType(), True),
                StructField("additional_field_4", StringType(), True)
            ])
            
            edges_df = spark.createDataFrame(edges_data, schema)
            
            # Salva in formato Parquet
            edges_df.write \
                .mode("overwrite") \
                .parquet(binary_graph_file)
            
            # 5. Gestisce il file dei gradi
            vertices_data = []
            map_vertices = single_attractor.m_c_graph.m_dict_vertices
            
            for vertex_id, vertex_value in map_vertices.items():
                degree = len(vertex_value.pNeighbours) - 1
                vertices_data.append(Row(vertex_id=vertex_id, degree=degree))
            
            # Crea DataFrame per i gradi
            degree_schema = StructType([
                StructField("vertex_id", IntegerType(), True),
                StructField("degree", IntegerType(), True)
            ])
            
            degree_df = spark.createDataFrame(vertices_data, degree_schema)
            
            temp_degfile = "temp_degfile"
            # Salva il file dei gradi
            degree_df \
                .select(concat_ws(" ", *degree_df.columns)) \
                .write \
                .mode("overwrite") \
                .text(temp_degfile)
            
            degree_df \
                .select(concat_ws(" ", *degree_df.columns)) \
                .coalesce(1) \
                .write \
                .mode("overwrite") \
                .text(temp_degfile)
            
            for filename in os.listdir(temp_degfile):
                if filename.startswith("part-"):
                    shutil.move(os.path.join(temp_degfile, filename), degfile_out)
                    break
            
            # 6. Pulizia
            if os.path.exists(local_temp_dir):
                shutil.rmtree(local_temp_dir)
                
            print("Calcolo della distanza di Jaccard completato con successo!")
            
        except Exception as e:
            print(f"Errore durante il calcolo: {str(e)}")
            raise
        finally:
            # Chiude la sessione Spark
            spark.stop()


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
