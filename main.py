import time
import signal, sys
from args_parser import parse_arguments
from attractor import GraphUtils
import gc
from attractor.GraphUtils import GraphUtils
from pyspark.sql import SparkSession
from attractor.NoConverged import NoConverged
from attractor.MRStarGraphWithPrePartitions import (
    MRStarGraphWithPrePartitions,
)
from attractor.MRPreComputePartition import MRPreComputePartition
from attractor.MRUpdateEdges import MRUpdateEdges
from libs.Graph import Graph
from attractor.CommunityDetection import CommunityDetection
from pyspark import SparkConf
from attractor.MRDynamicInteractions import (
    MRDynamicInteractions,
)
import warnings
from attractor.NoConverged import NoConverged
from rich import print
from libs.NewOutputFolder import delete_path
from typing import List, Tuple
from libs.Details import Details

warnings.filterwarnings("ignore")

DEBUG = False
REDUCED_EDGE = True

def log(message: str):
    print(f"[MRAttractor] {message}")

def main(args, spark, sc):
    delete_path(args.output_folder)
    details = Details(args.output_folder)
    details.main_start_timestamp = Details.current_timestamp()
    # -- PHASE 1: graph loading and computing jaccard Distance --
    graph_initilizer = GraphUtils()

    print(f"single_machine: {args.single_machine}")

    start_jaccard = time.time()
    graph_with_jaccard: Graph = graph_initilizer.init_jaccard(args.graph_file)
    n_v, n_e = graph_with_jaccard.get_num_vertex(), graph_with_jaccard.get_num_edges()

    log(f"[green]Loaded {args.graph_file}, |V|: {n_v}, |E|: {n_e} [/green]")
    rdd_graph_jaccard = graph_with_jaccard.get_graph_jaccard_rdd(spark, args.window_size)
    log(f"Jaccard distance init in {round(time.time() - start_jaccard, 2)} s")

    rdd_graph_jaccard.collect()  

    flag = True
    tic_main = time.time()
    counter = 0
    
    if args.single_machine == False:  

        # -- Compute Partitions --
        start_partition = time.time()
        df_partitioned = MRPreComputePartition.mapReduce(
            rdd_graph_jaccard, args.num_partitions
        )
        
        partitions = df_partitioned.collectAsMap()
        
        rdd_graph_jaccard = graph_with_jaccard.get_graph_jaccard_rdd(spark, args.window_size, partitions)
        
        del graph_with_jaccard
        gc.collect()
        
        log(f"Partitions computed in {round(time.time() - start_partition, 3)} s")

        while flag:
            # -- PHASE 2.1: Star Graph --
            rdd_star_graph = MRStarGraphWithPrePartitions.mapReduce(rdd_graph_jaccard)
            
            # -- PHASE 2.2: Dynamic Interactions --
            rdd_dynamic_interactions = MRDynamicInteractions.mapReduce(
                rdd_star_graph,
                args.num_partitions,
                args.lambda_,
            )

            # -- PHASE 2.3: Update Edges --
            rdd_updated_edges = MRUpdateEdges.mapReduce(
                rdd_dynamic_interactions,
                args.tau,
                args.window_size,
                counter,
            )

            # Actual execution of the 3 phases of MapReduce
            start_spark_execution = time.time()
            updated_edges = rdd_updated_edges.collect()
            
            reassing_partitions = []
            for (key, data) in updated_edges:
                center, target = key.split("-")
                center, target = int(center), int(target)
                new_data = (*data, tuple(partitions.get(center)), tuple(partitions.get(target))) 
                reassing_partitions.append((key, new_data))

            non_converged = NoConverged.reduce_edges(updated_edges)
            rdd_graph_jaccard = sc.parallelize(reassing_partitions)

            details.update_edges_timestamp.append(Details.current_timestamp())
            it_time = round(time.time() - start_spark_execution, 2)
            log(
                f"[bold orange3]It: {counter}, converged: {n_e - non_converged} / {n_e}, time {it_time} s [/bold orange3] "
            )

            flag = not (non_converged == 0)
            if non_converged < args.gamma:
                flag = False
                break

            counter += 1

    if args.single_machine:
        graph_jaccard: List[Tuple[str, Tuple[int, float, List[float], int, int]]] = rdd_graph_jaccard.collect()
        updated_edges = CommunityDetection.execute(
            graph_jaccard, args.window_size, args.lambda_, counter, details
        )
    elif non_converged < args.gamma:
        updated_edges = CommunityDetection.execute(
            updated_edges, args.window_size, args.lambda_, counter, details
        )

    log(f"Main time: [bold orange3] {round(time.time() - tic_main, 2)} [/bold orange3]")
    communities = NoConverged.connected_components(updated_edges, n_v, details)
    details.main_end_timestamp = Details.current_timestamp()
    # Save communities to file
    NoConverged.save_communities(communities, args.output_folder, n_v)
    details.save()

if __name__ == "__main__":
    args = parse_arguments()

    log(f"[cyan] Window size: {args.window_size}, gamma: {args.gamma} [/cyan]")

    try:
        conf = SparkConf()
        conf.setAppName("MRAttractor")
         # Enable detailed monitoring
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Detailed execution metrics
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        
        # Memory and GC monitoring
        conf.set("spark.executor.memory", "8g")
        conf.set("spark.driver.memory", "8g")
        conf.set("spark.executor.memoryFraction", "0.8")


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
