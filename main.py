import os
import time
from args_parser import parse_arguments
from attractor import GraphUtils
from attractor.GraphUtils import GraphUtils
from pyspark.sql import SparkSession
from attractor.MyUtil import MyUtil
from attractor.CleanUp import CleanUp
from attractor.MRStarGraphWithPrePartitions import (
    MRStarGraphWithPrePartitions,
)
from attractor.MRPreComputePartition import MRPreComputePartition
from attractor.MRUpdateEdges import MRUpdateEdges
from libs.Graph import Graph
from single_attractor.CommunityDetection import CommunityDetection
from pyspark import SparkConf
from attractor.RDD_to_DataFrame import (
    get_partitioned_dataframe,
    get_star_graph_dataframe,
    get_reduced_edges_dataframe,
)
from attractor.MRDynamicInteractions import (
    MRDynamicInteractions,
)
import warnings

warnings.filterwarnings("ignore")

DEBUG = False
REDUCED_EDGE = True

def main():
    
    
    print("Working directory:", os.getcwd())
   
    args = parse_arguments()
    time_generating_star_graph = 0.0
    time_computing_dynamic_interactions = 0.0
    time_updating_edges = 0.0
    time_running_on_single_machine = 0.0

    conf = SparkConf()
    conf.setAppName("MasterMR_Pipeline")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    local_filesystem = "local"
    MyUtil.delete_path(args.output_folder)

    # --------------------------------------------------------------------
    # -------------------- PHASE 1: Jaccard Distance ---------------------
    # --------------------------------------------------------------------

    graph_initilizer = GraphUtils(args.num_vertices)
    graph_with_jaccard: Graph = graph_initilizer.init_jaccard(args.graph_file)
    df_graph_jaccard = graph_with_jaccard.get_graph_jaccard_dataframe(spark)
    df_graph_degree = graph_with_jaccard.get_degree_dict()

    #print("Graph with Jaccard:", df_graph_jaccard.take(1))

    rdd_graph_degree_broadcasted = sc.broadcast(df_graph_degree)

    # --------------------------------------------------------------------
    # ---------------------- START compute partitions --------------------
    # --------------------------------------------------------------------

    tic_pre_partition = time.time()

    partition_computer = MRPreComputePartition()
    df_partitioned = partition_computer.mapReduce(
        df_graph_jaccard, args.num_partitions
    )
    #print(df_partitioned.collect())
    # df_partitioned = get_partitioned_dataframe(self.spark, partitioned)
    
    toc_pre_partition = time.time()
    pre_compute_partition_time = (toc_pre_partition - tic_pre_partition)
    print("pre_compute_partition_time:", round(pre_compute_partition_time, 3), "s")
    # --------------------------------------------------------------------
    # ---------------------- END compute partitions ----------------------
    # --------------------------------------------------------------------

    flag = True
    
    tic_main = time.time()
    iterations_counter = 0
    counter = 0
    while flag:
        
        # --------------------------------------------------------------------
        # ----------------------- PHASE 2.1: Star Graph ----------------------
        # --------------------------------------------------------------------
        
        tic = time.time()
        # Generate star graph with pre-partitions
        rdd_star_graph = MRStarGraphWithPrePartitions.mapReduce(
            df_graph_jaccard, df_partitioned, rdd_graph_degree_broadcasted
        )

        # df_star_graph = get_star_graph_dataframe(self.spark, rdd_star_graph)
        
        #print(rdd_star_graph.collect())

        toc = time.time()
        time_generating_star_graph += toc - tic
        #print("time_generating_star_graph:", round(time_generating_star_graph, 3), "s")
        
        # --------------------------------------------------------------------
        # ------------------- PHASE 2.2: Dynamic Interactions ----------------
        # --------------------------------------------------------------------

        tic = time.time()
        
        rdd_dynamic_interactions = MRDynamicInteractions.mapReduce(
            rdd_star_graph,
            args.num_partitions,
            args.lambda_,
            rdd_graph_degree_broadcasted,
        )
        
        toc = time.time()
        time_computing_dynamic_interactions += toc - tic
        #print("time_computing_dynamic_interactions:", round(time_computing_dynamic_interactions, 3), "s")
        
        # --------------------------------------------------------------------
        # ----------------------- PHASE 2.3: Update Edges --------------------
        # --------------------------------------------------------------------

        rdd_updated_edges = MRUpdateEdges.mapReduce(df_graph_jaccard, rdd_dynamic_interactions, args.tau, args.window_size, iterations_counter)

        
        start_spark_execution = time.time()
        
        # Actual execution of the 3 phases of MapReduce
        updated_edges = rdd_updated_edges.collect()
        
        print(f" >>> Total time iteration {round(time.time() - start_spark_execution , 3)} s <<< ")
        
        toc = time.time()
        time_updating_edges += toc - tic

        #print("time_updating_edges:", round(time_updating_edges, 3), "s")

        converged, non_converged, continued, reduced_edges = CleanUp.reduce_edges(args.num_vertices, updated_edges)
        df_reduced_edges = get_reduced_edges_dataframe(spark, reduced_edges)

        #print(df_reduced_edges.take(5))
        print(converged, non_converged, continued)
        new_graph = []
        for row in reduced_edges:
            new_graph.append((row.center, [{"type": "G", "target": row.target, "weight": row.weight}]))
        

        flag = not (non_converged == 0)
        df_graph_jaccard = sc.parallelize(new_graph)
        counter += 1
        print("Iteration number: ", counter)
        if flag == False:
            toc_main = time.time()
            print("Total time main:", round(toc_main - tic_main, 3), "s")

    if spark:
        spark.stop()
        print("SparkSession and SparkContext stopped")

if __name__ == "__main__":
    main()
