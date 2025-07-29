import os
import time
import signal, sys
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
    get_reduced_edges_dataframe,
)
from attractor.MRDynamicInteractions import (
    MRDynamicInteractions,
)
import warnings
from attractor.MyUtil import breadth_first_search, save_communities
from rich import print

warnings.filterwarnings("ignore")

DEBUG = False
REDUCED_EDGE = True


def log(message: str):
    print(f"[MRAttractor] {message}")


def main(args, spark, sc):
    MyUtil.delete_path(args.output_folder)

    # -- PHASE 1: graph loading and computing jaccard Distance --
    graph_initilizer = GraphUtils()

    start_jaccard = time.time()
    graph_with_jaccard: Graph = graph_initilizer.init_jaccard(args.graph_file)
    n_v, n_e = graph_with_jaccard.get_num_vertex(), graph_with_jaccard.get_num_edges()

    log(f"[green]Loaded {args.graph_file}, |V|: {n_v}, |E|: {n_e} [/green]")

    rdd_graph_jaccard = graph_with_jaccard.get_graph_jaccard_dataframe(spark)
    log(f"Jaccard distance init in {round(time.time() - start_jaccard, 2)} s")

    # Broadcast degree datasource
    rdd_graph_degree_broadcasted = sc.broadcast(graph_with_jaccard.get_degree_dict())

    # -- Compute Partitions --
    start_partition = time.time()
    df_partitioned = MRPreComputePartition.mapReduce(
        rdd_graph_jaccard, args.num_partitions
    ).persist()
    
    log(f"Partitions computed in {round(time.time() - start_partition, 3)} s")

    flag = True
    tic_main = time.time()
    counter = 0
    previousSlidingWindow = None
    while flag:
        # -- PHASE 2.1: Star Graph --
        rdd_star_graph = MRStarGraphWithPrePartitions.mapReduce(
            rdd_graph_jaccard, df_partitioned, rdd_graph_degree_broadcasted
        )

        # -- PHASE 2.2: Dynamic Interactions --
        rdd_dynamic_interactions = MRDynamicInteractions.mapReduce(
            rdd_star_graph,
            args.num_partitions,
            args.lambda_,
            rdd_graph_degree_broadcasted,
        )

        # -- PHASE 2.3: Update Edges --
        rdd_updated_edges, sliding_data = MRUpdateEdges.mapReduce(
            rdd_graph_jaccard,
            rdd_dynamic_interactions,
            args.tau,
            args.window_size,
            counter,
            previousSlidingWindow,
        )

        # Actual execution of the 3 phases of MapReduce
        start_spark_execution = time.time()
        updated_edges = rdd_updated_edges.collect()

        dict_sliding = sliding_data.collectAsMap()
        previousSlidingWindow = sc.broadcast(dict_sliding)

        converged, non_converged, continued, reduced_edges = CleanUp.reduce_edges(
            n_v, updated_edges
        )
        rdd_graph_jaccard = sc.parallelize(reduced_edges)


        it_time = round(time.time() - start_spark_execution, 2)
        log(
            f"[bold orange3]It: {counter}, converged: {converged} / {n_e}, time {it_time} s [/bold orange3] "
        )

        flag = not (non_converged == 0)
        if non_converged < args.gamma:
            flag = False

            # --------------------------------------------------------------------
            # ----------------------- PHASE 3: Community Detection ---------------
            # --------------------------------------------------------------------

            print("START Community Detection")
            singleMachineOutput = CommunityDetection.execute(
                reduced_edges, previousSlidingWindow, n_v
            )

            communities = breadth_first_search(singleMachineOutput, n_v)

        counter += 1

    log(f"Main time: [bold orange3] {round(time.time() - tic_main, 2)} [/bold orange3]")


    communities = breadth_first_search(reduced_edges, n_v)
    # Save communities to file
    save_communities(communities, args.output_folder, n_v)


if __name__ == "__main__":
    args = parse_arguments()

    log(f"[cyan] Window size: {args.window_size}, gamma: {args.gamma} [/cyan]")

    try:
        conf = SparkConf()
        conf.setAppName("MRAttractor")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark_context = spark.sparkContext

        spark_context.setLogLevel("ERROR")
        spark_context.setLocalProperty("spark.ui.showConsoleProgress", "false")

        def handle_sigint(signum, frame):
            print("\n[bold red] Execution interrupted, stopping spark... [/bold red]")
            try:
                if spark_context:
                    spark_context.cancelAllJobs()
                if spark:
                    spark.stop()
            except Exception:
                pass

        signal.signal(signal.SIGINT, handle_sigint)

        main(args, spark, spark_context)
    except (KeyboardInterrupt, Exception) as e:
        import traceback
        traceback.print_exc()
        log(f"[bold red] {e} [/bold red]")
        print("[bold red] Execution interrupted, stopping spark... [/bold red]")
    finally:
        if spark:
            spark.stop()
        sys.exit(0)
