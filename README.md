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

• Deeply analyze the MRAttractor algorithm and its theoretical foundation.  
• Implement a simplified or prototypical version of the algorithm for undirected, unweighted graphs.  
• Compare performance and results against other community detection algorithms.  
• Investigate possible optimizations or adaptations of the method.  

## Technologies and Tools

• **Languages**: Java  
• **Datasets**: SNAP (DBLP, YouTube, Amazon...)

## Brief explanation of the paper
**Objective**: to identify communities (groups of strongly connected nodes) in large graphs, such as those of social networks.

### Main Contributions of MRAttractor

**MapReduce (Hadoop) Distribution**: the algorithm was adapted to run in parallel across multiple nodes, enabling it to process massive graphs.  
**Sliding Window**: a technique to quickly determine whether an edge has “converged” in its distance, drastically reducing the number of iterations.  
**Smart Graph Partitioning**: the graph is divided into subgraphs that still allow the computation of interactions between indirectly connected nodes—a challenging task in distributed systems.  

### How MRAttractor Works (Briefly)

**Initialization**: the distance between each pair of connected nodes is computed using Jaccard distance.  
**Dynamic Interactions**: for each edge, distances are updated using:  
• **DI** *(Direct Interaction)*  
• **CI** *(Common Neighbor Interaction)*  
• **EI** *(Exclusive Neighbor Interaction)*  
**Sliding Window**: a sliding window of indicators (values -1 or 1) tracks how each edge's distance evolves, helping to predict convergence more quickly.  
**Community Extraction**: once all distances are either 0 or 1, edges with distance 1 are removed, and the connected components that remain represent the final communities.  

### What are Dynamic Interactions (DI, CI, EI)?

These interactions are used to adjust the distance between connected nodes in the graph, aiming to bring similar nodes closer and push dissimilar ones farther apart.  
There are three types:  
• **DI (Direct Interaction)**  
Measures the direct influence between two connected nodes.  
The more they “attract” each other, the more their distance decreases.  
It depends on the degrees of the nodes and the sine function of the distance complement:  
$$
DI(u, v) = \frac{\sin(1 - d(u, v))}{\deg(u)} + \frac{\sin(1 - d(u, v))}{\deg(v)}
$$

• **CI (Common Interaction)**  
Involves the common neighbors between two connected nodes.  
If common neighbors are strongly connected to both, the two nodes will be pulled closer together.  
More common neighbors with low distances → higher CI → stronger attraction.  

• **EI (Exclusive Interaction)**  
Concerns exclusive neighbors—those connected to only one of the two nodes.  
If an exclusive neighbor is similar to the other node (even if not directly connected), it influences the distance.  
This is calculated using similarity between unconnected nodes, which is a non-trivial task in a distributed environment.  

### What the Mapper and Reducer Do
MRAttractor is divided into three main phases, each implemented using MapReduce.   
These are the three MapReduce stages:  
1. *MR1 – Star Graph Generation*:  
**Goal**: Builds the local structure of each node (Γ(u)) to enable the computation of interactions.  
- **Mapper**:  
For each edge (u, v), it emits:  
⟨u; (v, d(u,v))⟩  
⟨v; (u, d(u,v))⟩  
This is used to build the star view around each node.  
- **Reducer**:  
It groups all the information about node u: its neighbors and their distances.  
Then it sorts the neighbors by ID → producing the “star graph” Γ(u) = {(v, d(u,v))}

2. *MR2 – Dynamic Interaction Computation*:  
**Goal**: computes DI, CI, and EI for each primary edge within the subgraphs G₍ᵢⱼₖ₎.  
- **Mapper**:  
For each Γ(u), it determines which subgraphs G<sub>ijk</sub> it should be used in.  
It emits ⟨(i,j,k); (u, Γ(u))⟩ → a copy of u’s star is sent to each relevant subgraph.  
- **Reducer**:  
It receives all Γ(u) for a given (i,j,k), which represents the local subgraph.  
It identifies the main edges, and for each one:    
  - Computes DI (direct interaction).   
  - Computes CI (common interaction).   
  - Computes EI (exclusive interaction, including rear edges).  
It emits ⟨(u,v); SI⟩ for each edge, where SI = DI + CI + EI  
3. *MR3 – Distance Update + Sliding Window*:  
**Goal**: updates the distance d(u,v) for each edge and determines whether it has converged.  
- **Mapper**:  
For each edge (u, v):  
It reads and emits:  
  - the previous distance d(u, v). 
  - the SI value computed in Phase 2. 
  - the sliding window vector w. 
- **Reducer**:  
It receives all the data related to the edge (u, v). 
It computes:  
  - the new distance: d(u, v) ← d(u, v) − SI. 
  - the updated sliding window vector w. 
  - If the trend clearly indicates convergence (many +1s or −1s), it forces the distance to 1 or 0.   
It emits the new values of d(u, v) and w for the next iteration

**Master Node (after MapReduce)**  
When the number of non-converged edges falls below a threshold γ, MRAttractor shifts the processing to the master node for more efficient final convergence.

## Python version  

```
- Install java 17
```

- main.py is the starting point of MRAttractor.
- To run main.py you should provide it arguments in following format:  
```  
python main.py testgraphs/karate.txt MrAttractor -1 0.6 -1 0 100 -1 34 20 0.7 5000 15000000 78 5 5 0
```

python main.py \
--graph-file testgraphs/karate.txt \
--output-folder MrAttractor \
--lambda 0.6 \
--window-size 20 \
--tau 0.7 \
--gamma 5000 \
--num-partitions 5 \
--single-machine False

```  
testgraphs/sample2.txt MrAttractor -1 0.5 -1 0 100 -1 3710 -20 0.7 5000 15000000 16453 5 5 0
```  

python main.py \
--graph-file testgraphs/sample2.txt \
--output-folder MrAttractor \
--lambda 0.5 \
--window-size -20 \
--tau 0.7 \
--gamma 5000 \
--num-partitions 5 \
--single-machine False