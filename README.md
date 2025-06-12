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
1. *MR1 – Star Graph Generation*: Builds the local structure of each node (Γ(u)) to enable the computation of interactions.  
2. *MR2 – Dynamic Interaction Computation*: computes DI, CI, and EI for each primary edge within the subgraphs G₍ᵢⱼₖ₎.  
3. *MR3 – Distance Update + Sliding Window*: updates the distance d(u,v) for each edge and determines whether it has converged.  

**Master Node (after MapReduce)**  
When the number of non-converged edges falls below a threshold γ, MRAttractor shifts the processing to the master node for more efficient final convergence.

### Number of Iterations  
The number of iterations depends on the dataset:  
• *YouTube*: 22 iterations  
• *Flixster*: 25 iterations  
• *Amazon*: 18 iterations  
• *DBLP*: ?  
