import os
import time
from args_parser import parse_arguments
from attractor import GraphUtils
from attractor.GraphUtils import GraphUtils
from pyspark.sql import SparkSession
from attractor.MyUtil import MyUtil
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

    # try:
    #     process = psutil.Process()
    #     memory_info = process.memory_info()
    #     total_mem = memory_info.rss
    #     megs = 1048576.0

    #     print(f"Total Memory: {total_mem} ({total_mem / megs:.2f} MiB)")
    #     print(
    #         f"Available Memory: {psutil.virtual_memory().available} ({psutil.virtual_memory().available / megs:.2f} MiB)"
    #     )
    # except ImportError:
    #     print("psutil not available - skipping memory info")

    local_filesystem = "local"
    MyUtil.delete_path(args.output_folder)

    # --------------------------------------------------------------------
    # -------------------- PHASE 1: Jaccard Distance ---------------------
    # --------------------------------------------------------------------

    graph_initilizer = GraphUtils(args.num_vertices)
    graph_with_jaccard: Graph = graph_initilizer.init_jaccard(args.graph_file)
    df_graph_jaccard = graph_with_jaccard.get_graph_jaccard_dataframe(spark).rdd
    df_graph_degree = graph_with_jaccard.get_degree_dict()

    #print("Graph with Jaccard:", df_graph_jaccard.take(1))

    rdd_graph_degree_broadcasted = sc.broadcast(df_graph_degree)

    # --------------------------------------------------------------------
    # ---------------------- START compute partitions --------------------
    # --------------------------------------------------------------------

    tic_pre_partition = time.time()

    partition_computer = MRPreComputePartition(spark)
    df_partitioned = partition_computer.mapReduce(
        df_graph_jaccard, args.num_partitions
    )
    # print(df_partitioned.collect())
    # df_partitioned = get_partitioned_dataframe(self.spark, partitioned)
    
    toc_pre_partition = time.time()
    pre_compute_partition_time = int((toc_pre_partition - tic_pre_partition) * 1000)
    
    # --------------------------------------------------------------------
    # ---------------------- END compute partitions ----------------------
    # --------------------------------------------------------------------

    using_sliding_window = int(args.window_size) > 0

    flag = True
    cnt_round = 0
    prev_converged_edges_file = ""
    tic = time.time()

    while flag:
        
        # --------------------------------------------------------------------
        # ----------------------- PHASE 2.1: Star Graph ----------------------
        # --------------------------------------------------------------------

        tic = time.time()
        # Generate star graph with pre-partitions
        star_graph = MRStarGraphWithPrePartitions(spark)
        rdd_star_graph = star_graph.mapReduce(
            df_graph_jaccard, df_partitioned, rdd_graph_degree_broadcasted
        )

        # df_star_graph = get_star_graph_dataframe(self.spark, rdd_star_graph)
        
        #print(rdd_star_graph.collect())

        toc = time.time()
        time_generating_star_graph += toc - tic
        
        # --------------------------------------------------------------------
        # ------------------- PHASE 2.2: Dynamic Interactions ----------------
        # --------------------------------------------------------------------

        tic = time.time()
        
        dynamic_interactions = MRDynamicInteractions(spark)
        rdd_dynamic_interactions = dynamic_interactions.mapReduce(
            rdd_star_graph,
            args.num_partitions,
            args.lambda_,
            rdd_graph_degree_broadcasted,
        )
        
        #print(rdd_dynamic_interactions.take(1))
        
        toc = time.time()
        time_computing_dynamic_interactions += toc - tic
        
        # --------------------------------------------------------------------
        # ----------------------- PHASE 2.3: Update Edges --------------------
        # --------------------------------------------------------------------

        print("START updating edges")
        tic = time.time()
        update_edges =  MRUpdateEdges(spark)
        rdd_updated_edges = update_edges.mapReduce(df_graph_jaccard, rdd_dynamic_interactions, args.tau, args.window_size)
        
        print(rdd_updated_edges.collect())
        print("END updating edges")
        exit(0)
        toc = time.time()
        time_updating_edges += toc - tic

        # Riduzione degli archi
        if REDUCED_EDGE:

            print(f"Reducing the number of edges @Loops: {cnt_round + 1}")
            info = reduce_edges(
                args.num_vertices,
                curr_edge_folder,
                local_filesystem,
                None,
                reduced_local_edge_file,
                current_left_edges_file,
                prev_converged_edges_file,
            )

            converged_edges, non_converged_edges, used_next_round_edges = info
            prev_converged_edges_file = current_left_edges_file
            curr_edge_folder = reduced_edges_folder

            # Tempo per questa iterazione
            ending_this_iteration_toc = time.time()
            length_each_iteration = (
                ending_this_iteration_toc - starting_iteration_tic
            )

            # Controlla se passare alla modalità single machine
            if non_converged_edges <= threshold_used_edges:
                tic_single_machine = time.time()
                dynamic_time_mr = (tic_single_machine - tic) * 1000

                # Esegui attractor su singola macchina
                single_machine_attractor = CommunityDetection(
                    int(args.windows_size),
                    float(args.tau),
                    float(args.lambda_),
                    cnt_round,
                    args.num_vertices,
                )
            
                tic_single_machine = time.time()

                single_machine_attractor.execute(
                    reduced_local_edge_file_normal,
                    merged_sliding_windows_file_normal,
                )

                prev_converged_edges_file = merged_all_converged_edges
                validate_final_edges(merged_all_converged_edges, args.num_edges)

                toc_single_machine = time.time()
                time_running_on_single_machine += (
                    toc_single_machine - tic_single_machine
                )
                flag = False
            else:
                # Copia il file ridotto su HDFS
                pass

        # Controlla se continuare
        if flag and MyUtil.path_exists(f"{out_update_edge}/flag"):
            flag = True
        else:
            print("MrAttractor-Hoi-Tu-Roi!!!!! Oh yeah")
            print(f"No-of-Loops: {cnt_round + 1}")
            toc = time.time()
            dynamic_time = int((toc - tic) * 1000)
            print(
                f"Total Running Time of Dynamic Interactions (single + MR) (seconds): {(toc - tic)}"
            )

            # BFS per trovare comunità
            tic = time.time()
            final_edge_file = prev_converged_edges_file
            toc = time.time()
            copying_time = int((toc - tic) * 1000)

            # Esegui BFS
            tic = time.time()
            breadth_first_search(final_edge_file, N, outfile)

            toc = time.time()
            breath_first_search_time = int((toc - tic) * 1000)

            three_phases = (
                initialization_time
                + dynamic_time
                + breath_first_search_time
                + copying_time
                + pre_compute_partition_time
                + find_workload_balancing_time
            )
            break

        cnt_round += 1

    if spark:
        spark.stop()
        print("SparkSession and SparkContext stopped")

if __name__ == "__main__":
    main()
