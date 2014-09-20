MRCube
======
In this project, I implemented MRCube algorithm in **"Distributed Cube Materialization on Holistic Measures"** of Arnab Nandi, Cong Yu, Philip Bohannon, Raghu Ramakrishnan.

Summary
======
 - Cube computation over massive datasets is critical for many important analyses done in the real world
 - **Algebraic measures** (i.e *SUM*) are easy to parallel. On the other hand, **holistic measures** (i.e *REACH, TOP-K*) is non-trivial.
 - In the paper, the authors identified an important subset of holistic measures and introduce MR-Cube algorithm for efficient cube computation on these measures.

Data cube analysis
======
Consider a warehouse: **<city, state, country, day, month, year, sales>** in which:
  
- *<city, state, country>*: location dimension
- *<day, month, year>*: temporal dimension
