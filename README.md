# Community Detection in Graphs: Study and Implementation of MRAttractor

## Overview

This project focuses on **community detection in undirected and unweighted graphs** , aiming to explore and implement effective techniques for identifying community structures in large-scale networks.

The initial phase is based on the analysis of the following research paper:  
**MRAttractor: Detecting Communities from Large-Scale Graphs**
*By Liu, Ye, et al.*
*IEEE Transactions on Knowledge and Data Engineering (2018)*  
*Link of the paper:* [arXiv:1806.08895](https://arxiv.org/abs/1806.08895)

MRAttractor extends the Attractor algorithm using the MapReduce paradigm, enabling it to scale effectively on massive datasets within distributed environments.

## Objectives

1. Deeply analyze the *MRAttractor* algorithm.  
2. Implement *MRAttractor* using ***Spark*** and test it on different datasets also investigate system load while running this algorithm

## The paper
### Main Contributions of MRAttractor

- **Designed to work with MapReduce**: the algorithm was adapted to run in parallel across multiple nodes, enabling it to process massive graphs.  
- **Sliding Window**: a technique to quickly determine whether an edge has “converged” in its distance, drastically reducing the number of iterations.  
- **Smart Graph Partitioning**: the graph is divided into subgraphs that still allow the computation of interactions between indirectly connected nodes, a challenging task in distributed systems.  

### How MRAttractor Works (Briefly)

1. **Initialization**: the distance between each pair of connected nodes is computed using Jaccard distance.  
2. **Dynamic Interactions**: for each edge, distances are updated using:  
    - **DI** *(Direct Interaction)*  
    - **CI** *(Common Neighbor Interaction)*  
    - **EI** *(Exclusive Neighbor Interaction)*  
3. **Sliding Window**: a sliding window of indicators (values -1 or 1) tracks how each edge's distance evolves, helping to predict convergence more quickly.  
4. **Community Extraction**: once all distances are either 0 or 1, edges with distance 1 are removed, and the connected components that remain represent the final communities.  

## Prerequisites  

- **Python 3.11**
- **PySpark 4.0.0**
- **Java 17**

## Usage

```bash
$ python3.11 -m venv .venv/
$ source .venv/bin/activate
$ pip install -r requirements.txt
```
### Run 

```bash
$ source .venv/bin/activate
$ python main.py  \
  --graph-file testgraphs/karate.txt \
  --output-folder MrAttractor \
  --lambda 0.5 \
  --window-size 15 \
  --tau 0.5 \
  --gamma 5000 \
  --num-partitions 10
```

### Parameters

- **graph-file**: Path to the social graph
- **output-folder**: Output folder where the results are saved
- **lambda**: Lambda factor
- **window-size**: Size of the windows, used to speed up convergence
- **tau**: Threshold for detecting convergence
- **gamma**: Max number of edges non conveged ramaining in order to switch from multi-processing to single processing (final faster convergence)
- **num-partitions**: Number of partition to split the graph, more partition more multi-processing

## Other scripts

- `check_communities.py`: extract qualitative metrics from the comparison between extracted communities and the top5000 of Stanford SNAP.
  ``` 
  python check_communities.py <output_folder>/details.npz testgraphs/<dataset>_top5000.txt>
  ``` 
- `system_load_wrapper.py`: wrapper that launch the `main.py`, observing and recording memory and cpu usage of it and its child processes. Accept the same arguments of `main.py`, but in abbreviate form.
