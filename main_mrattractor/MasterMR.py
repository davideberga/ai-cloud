import os
import sys
import time
import heapq
import shutil
import tempfile
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from collections import deque
from pyspark.sql import SparkSession
from MyUtil import MyUtil
from LoopGenStarGraphWithPrePartitions import LoopGenStarGraphWithPrePartitions
import LoopDynamicInteractionsFasterNoCache
from PreComputePartition import PreComputePartition
import LoopUpdateEdges
from single_attractor.CommunityDetection import CommunityDetection
from writable.Settings import Settings
from pyspark.sql.functions import concat_ws
from pyspark import SparkConf, SparkContext

class MasterMR:
    """
    Classe principale per il rilevamento di comunità tramite MapReduce
    """
    
    # Variabili di configurazione statiche
    DEBUG = False
    TEST_BALANCING = False
    FORFAST = False
    FORDICT = False
    CheckDegree = False
    REDUCED_EDGE = True
    DELETE_STALE_INFO = True
    PRE_COMPUTE_PARTITION = True
    ESTIMATION_WORK_LOAD = False
    BACKUP_DATA = False
    
    no_reducers_for_dynamic_interactions = 20
    degree_file_key = "deg_file"
    cache_type_message = ""
    LOAD_BALANCED_HEADER = "LOAD_BALANCED_HEADER"
    prefix_log = "CaiGiDoKhongOn-"
        
    def __init__(self):
        self.log_job = None
        self.time_generating_star_graph = 0.0
        self.time_computing_dynamic_interactions = 0.0
        self.time_updating_edges = 0.0
        self.time_running_on_single_machine = 0.0

        # Crea UNA SOLA SparkSession
        conf = SparkConf()
        conf.setAppName("MasterMR_Pipeline")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext  
    
    class Bucket:
        """Classe per gestire i bucket nel bilanciamento del carico"""
        
        def __init__(self, element):
            self.total_workload = element.workload
            self.subgraphs = [element.triple]
        
        def set(self, next_element):
            self.total_workload += next_element.workload
            self.subgraphs.append(next_element.triple)
        
        def __lt__(self, other):
            return self.total_workload < other.total_workload
        
        def __eq__(self, other):
            return self.total_workload == other.total_workload
    
    class SubGraphInfo:
        """Informazioni sui sottografi per il bilanciamento del carico"""
        
        def __init__(self, workload, triple):
            self.workload = workload
            self.triple = triple
        
        def __lt__(self, other):
            # Ordina dal più grande al più piccolo
            return self.workload > other.workload
        
        def __eq__(self, other):
            return self.workload == other.workload
    
    def gen_star_graphs_with_distance_pre_partition(self, inputs, output, degfile):
        """Genera grafi a stella con pre-partizioni"""
        tic = time.time()
        print("Generating Star Graphs+Distance+PrePartition.")
        
        # Simula l'esecuzione di ToolRunner
        tool = LoopGenStarGraphWithPrePartitions(self.spark)
        res = tool.run([inputs, output, degfile])
        
        toc = time.time()
        self.time_generating_star_graph += (toc - tic)
    
    def dynamic_interaction_faster(self, input_path, output, no_partitions, lambda_val, 
                                 no_loops, s_cache_size, mb_per_reducers, no_reducers,
                                 degfile, hdfs_load_balanced_file):
        """Esegue le interazioni dinamiche"""
        tic = time.time()
        print("Dynamic Interactions No Cache.")
        self.cache_type_message = "Dynamic+Interactions+No+Cache"
        
        tool = LoopDynamicInteractionsFasterNoCache()
        res = tool.run([input_path, output, no_partitions, lambda_val, no_loops,
                       s_cache_size, mb_per_reducers, no_reducers, degfile, 
                       hdfs_load_balanced_file])
        
        toc = time.time()
        self.time_computing_dynamic_interactions += (toc - tic)
    
    def precompute_partitions(self, input_path, output, no_partitions):
        """Pre-calcola le partizioni del grafo"""
        print("Pre-computing Partition of Graph.")
        tool = PreComputePartition(self.spark)
        res = tool.run([input_path, output, no_partitions])
    
    def update_edge(self, input_path, output, no_loops, windows_size, miu):
        """Aggiorna gli archi"""
        tic = time.time()
        print("Update Edges.")
        tool = LoopUpdateEdges()
        res = tool.run([input_path, output, no_loops, miu, windows_size])
        toc = time.time()
        self.time_updating_edges += (toc - tic)
    
    def compute_load_balanced_greedy(self, workload_estimate_out, graphname, 
                                   hdfs, localsystem, conf, no_reducer_dynamic, 
                                   balanced_result_file):
        """Calcola il bilanciamento del carico usando algoritmo greedy"""
        out_est_load_file_merged = f"{graphname}_est_workload_merged.txt"
        MyUtil.merge_files(workload_estimate_out, out_est_load_file_merged, 
                          hdfs, localsystem, conf)
        
        no_reducers = int(no_reducer_dynamic)
        list_workload = []
        
        with open(out_est_load_file_merged, 'r') as reader:
            for line in reader:
                args = line.strip().split()
                workload = int(args[3])
                triple = f"{args[0]} {args[1]} {args[2]}"
                ele = self.SubGraphInfo(workload, triple)
                list_workload.append(ele)
        
        os.remove(out_est_load_file_merged)
        
        K = no_reducers
        N = len(list_workload)
        
        with open(balanced_result_file, 'w') as writer:
            if K >= N:
                # Quando ci sono abbastanza reducer
                for i, subgraph_info in enumerate(list_workload):
                    subgraph = subgraph_info.triple
                    writer.write(f"{subgraph} {i}\n")
            else:
                # Quando abbiamo pochi reducer, bilanciamo il carico
                list_workload.sort()  # Ordina dal più grande al più piccolo
                
                # Inizializza K bucket nell'heap
                workload_heap = []
                for i in range(K):
                    sub = list_workload[i]
                    heapq.heappush(workload_heap, self.Bucket(sub))
                
                # Approccio greedy
                for i in range(K, N):
                    sub = list_workload[i]
                    # Seleziona il bucket più piccolo
                    top_smallest = heapq.heappop(workload_heap)
                    
                    if self.DEBUG:
                        if workload_heap:
                            next_bucket = workload_heap[0]
                            assert next_bucket.total_workload >= top_smallest.total_workload
                    
                    top_smallest.set(sub)
                    heapq.heappush(workload_heap, top_smallest)
                    assert len(workload_heap) == K
                
                if self.TEST_BALANCING:
                    with open("check_load_balancing", 'w') as test_heap:
                        test_heap.write(f"Number of reducers: {K}\n")
                        for bucket in workload_heap:
                            test_heap.write(f"{bucket.total_workload}\n")
                
                # Scrivi i risultati
                for i in range(K):
                    bucket = heapq.heappop(workload_heap)
                    for s in bucket.subgraphs:
                        writer.write(f"{s} {i}\n")
    
    def init_log_file(self, graphfile, cache_type, no_partition_dynamic_interaction, 
                     no_reducers):
        """Inizializza il file di log"""
        a = graphfile.split("/")
        namefile = "+".join(a)
        
        cache_type_messages = {
            0: "Dynamic+Interactions+No+Cache",
            1: "Dynamic+Interactions+With+Cache+Common+Nodes",
            2: "Dynamic+Interactions+With+Cache+Total+Degree",
            3: "Dynamic+Interactions+Cache+No+Priority+Queue",
            4: "DynamicInteractions+SharedMemoryCache+NoPriorityQueue"
        }
        
        if cache_type in cache_type_messages:
            self.cache_type_message = cache_type_messages[cache_type]
        else:
            raise NotImplementedError("Not implemented for other cases!!!")
        
        log_filename = f"stat-MrAttractor-{namefile}-{self.cache_type_message}-partions-{no_partition_dynamic_interaction}-noReducers-{no_reducers}.txt"
        self.log_job = open(log_filename, 'w')
    
    def reduce_edges(self, N, curr_edge_folder, hdfs, local, conf, 
                    reduced_local_edge_file, left_edges_file, prev_left_edges_file):
        """Riduce il numero di archi dopo ogni iterazione"""
        try:
            tic = time.time()
            
            # Inizializza tutti i vertici come virgin
            dirty = [0] * N
            non_converged_edges = 0
            converged_edges = 0
            
            local_edge_folder = os.path.basename(curr_edge_folder)
            
            # Copia la cartella
            MyUtil.copy_folder(curr_edge_folder, local_edge_folder, hdfs, local, conf)
            
            # Leggi tutti i file di edge
            list_edges = []
            for filename in os.listdir(local_edge_folder):
                if not filename.startswith('.'):
                    file_path = os.path.join(local_edge_folder, filename)
                    # Leggi il file sequenziale (simulato)
                    edges = MyUtil.read_sequence_file(file_path)
                    list_edges.extend(edges)
            
            # Rimuovi la cartella scaricata
            MyUtil.fully_delete(local_edge_folder)
            
            # Prima passata: identifica vertici dirty
            for edge in list_edges:
                u = edge.center
                v = edge.target
                dis = edge.weight
                
                if 0 < dis < 1:
                    u -= 1
                    v -= 1
                    dirty[u] = 1
                    dirty[v] = 1
                    non_converged_edges += 1
                else:
                    converged_edges += 1
            
            # Seconda passata: marca vertici adiacenti ai dirty
            for edge in list_edges:
                u = edge.center - 1
                v = edge.target - 1
                
                if dirty[u] == 1 or dirty[v] == 1:
                    if dirty[u] == 0:
                        dirty[u] = 2
                    if dirty[v] == 0:
                        dirty[v] = 2
            
            the_number_of_continued_to_used_edges = 0
            
            # Scrivi gli archi che continuano
            with open(reduced_local_edge_file, 'w') as writer:
                with open(left_edges_file, 'w') as left_edges_writer:
                    for edge in list_edges:
                        u = edge.center - 1
                        v = edge.target - 1
                        
                        if dirty[u] in [1, 2] or dirty[v] in [1, 2]:
                            # Scrivi nell'output binario (simulato)
                            writer.write(f"{edge.center} {edge.target} {edge.weight}\n")
                            the_number_of_continued_to_used_edges += 1
                        else:
                            left_edges_writer.write(edge.to_string_for_local_machine() + "\n")
                    
                    # Aggiungi archi dal file precedente se esiste
                    if prev_left_edges_file and os.path.exists(prev_left_edges_file):
                        with open(prev_left_edges_file, 'r') as reader1:
                            for line in reader1:
                                left_edges_writer.write(line)
                        
                        os.remove(prev_left_edges_file)
            
            message = f"#Converged edges: {converged_edges} #Edges of reduced graph: {the_number_of_continued_to_used_edges} #non-converged edges: {non_converged_edges}"
            print(message)
            self.log_job.write(message + "\n")
            self.log_job.flush()
            
            toc = time.time()
            self.time_updating_edges += (toc - tic)
            
            return [converged_edges, non_converged_edges, the_number_of_continued_to_used_edges]
            
        except Exception as e:
            print(f"Error in reduce_edges: {e}")
            sys.exit(1)
    
    def merge_two_files(self, file1, file2, outputfile):
        """Unisce due file"""
        with open(outputfile, 'w') as writer:
            # Copia file1
            with open(file1, 'r') as reader:
                for line in reader:
                    writer.write(line)
            
            # Copia file2
            with open(file2, 'r') as reader:
                for line in reader:
                    writer.write(line)
        
        # Rimuovi i file originali
        os.remove(file1)
        os.remove(file2)
    
    def validate_final_edges(self, file1, no_original_edges):
        """Valida gli archi finali"""
        cnt = 0
        with open(file1, 'r') as reader:
            for line in reader:
                args = line.strip().split()
                u = int(args[0])
                v = int(args[1])
                dis = float(args[2].replace(',', '.'))
                
                assert abs(dis) < 1e-10 or abs(dis - 1.0) < 1e-10
                cnt += 1
        
        assert cnt == no_original_edges
    
    def breadth_first_search(self, filename, N, outfile):
        """Ricerca in ampiezza per trovare le comunità"""
        E = 0
        n_edge_dis1 = 0
        visited = [0] * N
        comms = [0] * N
        adj_list = [[] for _ in range(N)]
        
        # Leggi il file degli archi finali
        with open(filename, 'r') as reader:
            for line in reader:
                args = line.strip().split()
                u = int(args[0])
                v = int(args[1])
                
                n = args[2].replace(',', '.')
                if '0.' in n:
                    dis = 0
                elif '1.' in n:
                    dis = 1
                else:
                    raise AssertionError("Something wrong with implementation")
                
                assert 1 <= u <= N and 1 <= v <= N
                E += 1
                
                if dis == 1:
                    n_edge_dis1 += 1
                    continue
                
                u -= 1  # Converti a indice 0-based
                v -= 1
                adj_list[u].append(v)
                adj_list[v].append(u)
        
        # Esegui BFS
        queue = deque()
        ID = 0
        
        for i in range(N):
            if visited[i] == 0:
                # Inizia nuovo BFS
                queue.clear()
                visited[i] = 1
                queue.append(i)
                ID += 1  # Nuova comunità
                comms[i] = ID
                
                while queue:
                    curr = queue.popleft()
                    assert 0 <= curr < N
                    visited[curr] = 1
                    
                    for adj in adj_list[curr]:
                        if visited[adj] == 1:
                            continue
                        visited[adj] = 1
                        comms[adj] = ID
                        queue.append(adj)
        
        # Scrivi output
        with open(outfile, 'w') as out:
            for i in range(N):
                out.write(f"{i+1} {comms[i]}\n")
    
    def master(self, graphfile, outfolder, no_partitions, lambda_val, s_cache_size,
              cache_type, mb_per_reducers, no_reducers_unused, 
              no_reducers_dynamic_interaction, N, M, windows_size, miu,
              threshold_used_edges, cache_size_single_attractor,
              no_partition_dynamic_interaction):
        """Metodo principale del master"""
        
        local_filesystem = "local"
        
        if self.DELETE_STALE_INFO:
            MyUtil.delete_path(outfolder)

        tic = time.time()
        
        a = graphfile.split("/")
        backup_folder = f"backup_folder_{a[-1]}"
        if self.BACKUP_DATA:
            os.makedirs(backup_folder, exist_ok=True)
        
        # Inizializza file di log
        self.init_log_file(graphfile, cache_type, no_partition_dynamic_interaction, 
                          no_reducers_dynamic_interaction)
        
        prefix = outfolder
        degfile = f"{prefix}/degfile.txt"
        out_distance_init = f"{prefix}/phase2"
        curr_edge_folder = f"{out_distance_init}/edges"
        
        # Inizializzazione distanza
        if not os.path.exists(curr_edge_folder):
            os.makedirs(curr_edge_folder)
        
        binary_graph_file = f"{curr_edge_folder}/binary_graph_file_initialized"
            
        single_attractor = MyUtil.compute_jaccard_distance_single_machine(
            graphfile, binary_graph_file, N, M, float(lambda_val), degfile)
        
        # 1. Prepara il file del grafo
        downloaded_graph_file = os.path.basename(graphfile)
        local_temp_dir = tempfile.mkdtemp()
        
        #print(f"graph_file: {graphfile}")
        #print(f"downloaded_graph_file: {downloaded_graph_file}")

        # 2. Prepara i dati per il salvataggio in formato Spark
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
        
        # 3. Crea DataFrame Spark 
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
        
        edges_df = self.spark.createDataFrame(edges_data, schema)

        temp_binary_file = "temp_binary_file"

        edges_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "\t") \
            .option("header", "true") \
            .csv(temp_binary_file)
        
        for filename in os.listdir(temp_binary_file):
            if filename.startswith("part-"):
                shutil.move(os.path.join(temp_binary_file, filename), binary_graph_file)
                break
        
        # 4. Gestisce il file dei gradi
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
        
        degree_df = self.spark.createDataFrame(vertices_data, degree_schema)
        
        temp_degfile = "temp_degfile"

        # Salva il file dei degrees
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
                shutil.move(os.path.join(temp_degfile, filename), degfile)
                break
        
        # 5. Pulizia
        if os.path.exists(local_temp_dir):
            shutil.rmtree(local_temp_dir)
        
        toc = time.time()
        initialization_time = int((toc - tic) * 1000)
        
        self.log_job.write(f"Init Interaction Time {initialization_time / 1000.0}\n")
        self.log_job.flush()
        print(f"Running Time of Initialization: {initialization_time / 1000.0}")
        
        # --------------------------------------------------------------------
        # ---------------------- START calcolo partizioni --------------------
        # --------------------------------------------------------------------

        tic_pre_partition = time.time()
        partitions_out = f"{prefix}/partitions"
        
        if self.PRE_COMPUTE_PARTITION:
            self.precompute_partitions(curr_edge_folder, partitions_out, 
                                     no_partition_dynamic_interaction)
        
        toc_pre_partition = time.time()
        pre_compute_partition_time = int((toc_pre_partition - tic_pre_partition) * 1000)
        self.log_job.write(f"Running time of Pre-partition of all edges: (seconds){pre_compute_partition_time/1000.0} p={no_partition_dynamic_interaction}\n")
        self.log_job.flush()

        # --------------------------------------------------------------------
        # ---------------------- END calcolo partizioni ----------------------
        # --------------------------------------------------------------------
        
        using_sliding_window = int(windows_size) > 0
        current_sliding_window_folder = f"{prefix}/sliding_empty"
        if not os.path.exists(current_sliding_window_folder):
            os.makedirs(current_sliding_window_folder)
        
        # Loop principale delle interazioni dinamiche
        flag = True
        cnt_round = 0
        cache_size = int(s_cache_size)
        prev_converged_edges_file = ""
        tic = time.time()
        
        while flag:
            print(f"Current Loop: {cnt_round + 1}")
            out_star_graph_with_dist = f"{prefix}/LoopPhase1"
            
            starting_iteration_tic = time.time()
            
            # Stima del carico di lavoro
            tic_estimation = time.time()
            hdfs_load_balanced_file = f"{prefix}/loadEstimated/load_balanced_estimated.txt"
            toc_estimation = time.time()
            find_workload_balancing_time = int((toc_estimation - tic_estimation) * 1000)
            self.log_job.write(f"Running time of Estimation WorkLoad: (seconds) {find_workload_balancing_time / 1000.0} p={no_partition_dynamic_interaction}\n")
            self.log_job.flush()
            
            # Genera grafi a stella con pre-partizioni
            inputs_for_gen_star_graphs = f"{curr_edge_folder},{partitions_out}"
            self.gen_star_graphs_with_distance_pre_partition(
                inputs_for_gen_star_graphs, out_star_graph_with_dist, degfile)
            
            # Interazioni dinamiche
            out_dynamic = f"{prefix}/LoopPhase2"
            no_loops = str(cnt_round)
            
            if cache_type == 0:
                self.dynamic_interaction_faster(
                    f"{out_star_graph_with_dist}/star", out_dynamic,
                    no_partition_dynamic_interaction, lambda_val, no_loops,
                    s_cache_size, mb_per_reducers, no_reducers_dynamic_interaction,
                    degfile, hdfs_load_balanced_file)
            else:
                raise NotImplementedError("Not implemented for other cases!!!")
            
            # Aggiorna archi
            out_update_edge = f"{prefix}/LoopPhase3_{no_loops}"
            
            if self.DEBUG:
                input_str = f"{curr_edge_folder};{out_dynamic}/delta_dis;{out_dynamic}/debug;{current_sliding_window_folder}"
            else:
                input_str = f"{out_dynamic}/delta_dis;{curr_edge_folder};{current_sliding_window_folder}"
                # Rimuovi cartella precedente per risparmiare spazio
                nloop = int(no_loops)
                if nloop != 0:
                    tt = f"{prefix}/LoopPhase3_{nloop-1}"
                    MyUtil.delete_path(tt)
            
            self.update_edge(input_str, out_update_edge, no_loops, windows_size, miu)
            
            if using_sliding_window:
                current_sliding_window_folder = f"{out_update_edge}/sliding"
            
            curr_edge_folder = f"{out_update_edge}/edges"
            
            # Riduzione degli archi
            if self.REDUCED_EDGE:
                current_left_edges_file = f"converged_edges_{a[-1]}_{self.cache_type_message}_{cnt_round + 1}"
                reduced_local_edge_file = f"local_edge_file_{a[-1]}_reduced_local_{cnt_round + 1}.txt"
                reduced_edges_folder = f"{out_update_edge}/reduced_edges"
                
                print(f"Reducing the number of edges @Loops: {cnt_round + 1}")
                info = self.reduce_edges(N, curr_edge_folder, local_filesystem, None,
                                       reduced_local_edge_file, current_left_edges_file,
                                       prev_converged_edges_file)
                
                converged_edges, non_converged_edges, used_next_round_edges = info
                prev_converged_edges_file = current_left_edges_file
                curr_edge_folder = reduced_edges_folder
                
                # Tempo per questa iterazione
                ending_this_iteration_toc = time.time()
                length_each_iteration = ending_this_iteration_toc - starting_iteration_tic
                self.log_job.write(f"Running time of {cnt_round + 1} iteration: {length_each_iteration}\n")
                self.log_job.flush()
                
                # Controlla se passare alla modalità single machine
                if non_converged_edges <= threshold_used_edges:
                    tic_single_machine = time.time()
                    dynamic_time_mr = (tic_single_machine - tic) * 1000
                    self.log_job.write("Starting Entering Single Mode\n")
                    self.log_job.write(f"No of Loops of MR: {cnt_round + 1}\n")
                    self.log_job.write(f"Dynamic Interaction Time of MR version: {dynamic_time_mr / 1000.0}\n")
                    self.log_job.flush()
                    
                    output_single_machine = f"output_edges_{a[-1]}single_machine"
                    
                    # Esegui attractor su singola macchina
                    single_machine_attractor = CommunityDetection(
                        int(windows_size), float(miu), float(lambda_val),
                        cache_size_single_attractor, cnt_round, N,
                        output_single_machine, self.log_job)
                    
                    reduced_local_edge_file_normal = f"{reduced_local_edge_file}_normal"
                    MyUtil.convert_binary_file_to_text_file(
                        reduced_local_edge_file, reduced_local_edge_file_normal, local_filesystem)
                    
                    tic_single_machine = time.time()
                    merged_sliding_windows_file_normal = ""
                    
                    if using_sliding_window:
                        binary_sliding_windows_folder_local = f"sliding_merged_{a[-1]}MR"
                        MyUtil.copy_folder(current_sliding_window_folder, binary_sliding_windows_folder_local)
                        merged_sliding_windows_file_normal = f"sliding_merged_{a[-1]}_normal"
                        MyUtil.merge_and_convert_binary_files(
                            binary_sliding_windows_folder_local, 
                            merged_sliding_windows_file_normal)
                    
                    single_machine_attractor.execute(reduced_local_edge_file_normal, 
                                                   merged_sliding_windows_file_normal)
                    
                    merged_all_converged_edges = f"mergedAllConvergedEdges_{a[-1]}.edges"
                    self.merge_two_files(prev_converged_edges_file, output_single_machine, 
                                       merged_all_converged_edges)
                    prev_converged_edges_file = merged_all_converged_edges
                    self.validate_final_edges(merged_all_converged_edges, M)
                    
                    toc_single_machine = time.time()
                    self.time_running_on_single_machine += (toc_single_machine - tic_single_machine)
                    flag = False
                else:
                    # Copia il file ridotto su HDFS
                    reduced_hdfs_edge_file = f"{reduced_edges_folder}/reduced_edges.txt"
                    MyUtil.copy_file(reduced_local_edge_file, reduced_hdfs_edge_file)
                    if self.DELETE_STALE_INFO:
                        os.remove(reduced_local_edge_file)
            
            # Controlla se continuare
            if flag and MyUtil.path_exists(f"{out_update_edge}/flag"):
                flag = True
            else:
                print("MrAttractor-Hoi-Tu-Roi!!!!! Oh yeah")
                print(f"No-of-Loops: {cnt_round + 1}")
                toc = time.time()
                dynamic_time = int((toc - tic) * 1000)
                print(f"Total Running Time of Dynamic Interactions (single + MR) (seconds): {(toc - tic)}")
                
                self.log_job.write("Finally, everything converges!!!!! Oh yeah\n")
                self.log_job.write(f"No of Loops: {cnt_round + 1}\n")
                self.log_job.write(f"Init Interaction Time (duplicated info){initialization_time / 1000.0}\n")
                self.log_job.write(f"Dynamic Interaction Time (single + MR) (seconds) {dynamic_time / 1000.0}\n")
                self.log_job.write(f"Running time generating star graphs: {self.time_generating_star_graph}\n")
                self.log_job.write(f"Running time computing dynamic interactions: {self.time_computing_dynamic_interactions}\n")
                self.log_job.write(f"Running time updating edges: {self.time_updating_edges}\n")
                self.log_job.write(f"Running time on single machine: {self.time_running_on_single_machine}\n")
                
                # BFS per trovare comunità
                tic = time.time()
                final_edge_file = prev_converged_edges_file
                toc = time.time()
                copying_time = int((toc - tic) * 1000)
                self.log_job.write(f"Copying Final Edge file: {copying_time / 1000.0}\n")
                
                # Esegui BFS
                tic = time.time()
                outfile = f"{final_edge_file}.communities"
                self.breadth_first_search(final_edge_file, N, outfile)
                
                toc = time.time()
                breath_first_search_time = int((toc - tic) * 1000)
                self.log_job.write(f"Breadth First Search Time: {breath_first_search_time / 1000.0}\n")
                
                three_phases = (initialization_time + dynamic_time + breath_first_search_time + 
                               copying_time + pre_compute_partition_time + find_workload_balancing_time)
                self.log_job.write(f"Total Running Time (seconds): {three_phases / 1000.0}\n")
                self.log_job.close()
                break
            
            cnt_round += 1

        """Chiudi SparkSession (chiude anche SparkContext automaticamente)"""
        if self.spark:
            self.spark.stop()
            print("SparkSession and SparkContext stopped")

def main():
    """
    Main function equivalent to the Java MasterMR.main method
    """
    print("Working directory:", os.getcwd())
    
    # Memory information (equivalent to Java Runtime memory checks)
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        total_mem = memory_info.rss
        megs = 1048576.0
        
        print(f"Total Memory: {total_mem} ({total_mem/megs:.2f} MiB)")
        print(f"Available Memory: {psutil.virtual_memory().available} ({psutil.virtual_memory().available/megs:.2f} MiB)")
    except ImportError:
        print("psutil not available - skipping memory info")
    
    # Check arguments
    args = sys.argv[1:]  # Exclude script name
    if len(args) < 16:
        raise AssertionError(f"Number of arguments must be >=16 while current input has: {len(args)} arguments.")
    
    # Parse arguments
    graphfile = args[0]
    outfolder = args[1]
    nopartitions = args[2]
    lambda_val = args[3]
    cache_size = args[4]
    cache_type = int(args[5])
    mb_per_reducers = args[6]
    no_reducers = args[7]
    no_vertices = int(args[8])
    windows_size = args[9]
    miu = args[10]  # Percentage of 1 or 0 in a sliding windows to consider an edge converged
    no_edges_to_run_single_machine = int(args[11])  # The number of edges that are small enough to run on single machine Attractor
    cache_size_single_attractor = int(args[12])  # The maximum size of dictionary. This is to avoid out-of-memory of Master node
    no_edges = int(args[13])
    no_reducers_dynamic_interaction = args[14]
    no_partition_dynamic_interaction = args[15]
    
    # Validate cache type
    if cache_type != 0:
        raise AssertionError(
            "caching type value must be in {0, 1,2,3}. \n"
            "[0] means no caching at all \n "
        )
    
    # Create MasterMR instance and call master method
    
    master_mr = MasterMR()
    
    master_mr.master(
        graphfile=graphfile,
        outfolder=outfolder,
        no_partitions=nopartitions,
        lambda_val=lambda_val,
        s_cache_size=cache_size,
        cache_type=cache_type,
        mb_per_reducers=mb_per_reducers,
        no_reducers_unused=no_reducers,
        no_reducers_dynamic_interaction=no_reducers_dynamic_interaction,
        N=no_vertices,
        M=no_edges,
        windows_size=windows_size,
        miu=miu,
        threshold_used_edges=no_edges_to_run_single_machine,
        cache_size_single_attractor=cache_size_single_attractor,
        no_partition_dynamic_interaction=no_partition_dynamic_interaction
    )

if __name__ == "__main__":
    main()