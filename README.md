MRCube
======
In this project, I implemented MRCube algorithm in **"Distributed Cube Materialization on Holistic Measures"** of *Arnab Nandi, Cong Yu, Philip Bohannon, Raghu Ramakrishnan*.

Summary
======
 - Cube computation over massive datasets is critical for many important analyses done in the real world
 - **Algebraic measures** (i.e *SUM*) are easy to parallel. On the other hand, **holistic measures** (i.e *REACH, TOP-K*) is non-trivial.
 - In the paper, the authors identified an important subset of holistic measures and introduce MR-Cube algorithm for efficient cube computation on these measures.

Data cube analysis
======
Consider a warehouse: **(city, state, country, day, month, year, sales)** in which:
- *(city, state, country)*: location dimension
- *(day, month, year)*: temporal dimension
Cube analysis computes aggregate measures (e.g *sales*) over all possible groups defined by the two dimesions. 

There are two main limitations in the existing techniques:
- They designed for a **single machine** or **clusters with small number of nodes**. With the growing of data (terabytes accumalted per day), it is **difficult** to **process** data with that infrastructure.
- Many of them **takes advantage** of the measure being **algebraic**.

How to **efficiently extend** cube analysis for **holistic measures** in **Map Reduce paradigm**? Existing problems:
- *Effective distribute data*: avoid overhemlmed for any single machine --> address by **identifying the partially algebraic measures** and **value partition mechanism**.
- *Effective distribute computation*: good balance between the amount of intermediate data being produced and the pruning unnessary data --> adress by **batch areas** 

Definitions
======
- **Dimension attributes**: attributes that users want to analyze
- **Cube lattice**: all possible grouping(s) of the attributes
- **Cube region**: each node in cube lattice represents one possible grouping 
- **Cube group**: an actual tuple belonging to a cube region.
- Each edge in the lattice represents a parent/child relationship between two cube regions or two cube groups
- **Cubing task**: is to compute given measures for all valid cube groups
- **Algebraic & Holistic & monotomic**: find in the paper for the formal definitions

Challenges
======
*Cube expressed in Pig* by disjunction of groupy querys, then it combines all queries into a single MapReduce job. This approache is simple but effiencient for small datasets. But when the scale of data increases, this algorithm to perform poorly and eventually fail: size of intermediate data and size of large groups.

- Size of Intermediate Data: |C| * |D|, where |C| is the number of regions in the cube lattice and |D| is the size of the input data
- Size of Large Groups: The reducer that is assigned the cube regions at the bottom part of the cube lattice essentially has to compute the measure for the entire dataset, which is usually large enough to cause the reducer to take significantly longer time to finish than others or even fail. For algebraic measures, this challenge can addressed by not processing those groups directly: we can first compute measures only for those smaller, reducer-friendly, groups, then combine those measures to produce the measure for the larger, reducer-unfriendly, groups. Such measures are also amenable to mapper-side aggregation which further decreases the load on the shuffle and reduce phases. For holistic measures, however, measures for larger groups can not be assembled from their smaller child groups, and mapper-side aggregation is also not possible. Hence, we need a different approach.

The MR-Cube approach
======
**Note**: the complexity of cubing tasks depend on:
- **data size**: impacts intermediate data size, the size of large group
- **cube lattice size** (is controlled by the number/depth of dimensions impacts intermediate data size

MR-Cube approach deal with those complexities in a two-pronged attack: **data partitioning** and **cube lattice partitioning**

Partially Algebraic Measures
======
- Purpose: to identify a subset of holistic measures that are easy to compute in **parallel** than an arbitrary holistic measure.
- We call this technique of partitioning large groups based on the algebraic attribute **value partitioning**, and the ratio by which a group is partitioned the **partition factor**

Value Partitioning
======
An easy way to accomplish value partitioning is to run the naive algorithm, but further **partition each cube group based on the algebraic attribute**. The number of map keys being produced is now **the product of the number of groups and the partition factor**.

Observations:
- Many of the original groups contain a manageable number of tuples and partitioning those groups is entirely unnecessary
- Even for large, reducer- unfriendly, groups, some will require partitioning into many sub-groups (i.e., large partition factor), while others will only need to be partitioned into a few sub-groups

The idea is to perform value partitioning only on groups that are likely to be **reducer-unfriendly** and **dynamically adjust the partition factor**

Approaches:
- Detect reducer unfriendly groups on the fly and perform partitioning upon detection -> overwhelm the mapper since it requires us to maintain information about groups visited.
- Scan the data and compile a list of potentially reducer-unfriendly groups for which the mapper will perform partitioning -> Checking against a potentially large list slows down the mapper.

Based on these analyses, the authors proposed **sampling approach**

Sampling Approach
======
- Estimate the **reducer-unfriendliness** of each **cube region** based on **the number of groups it is estimated to have**
- Perform **partitioning for all groups** within the list of cube regions (a small list) that are estimated to be reducer **unfriendly**