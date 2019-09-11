# Project Description

This project is extends the Breath First Search algorithm for graph traversal to compute the K-source shortest path of a massive graph in a parallel environment. 

The biggest challenge during the development of this was the inability to have a shared memory location, since each worker machine works in isolation, independent of other machines. Since there is no shared space, the concept of having an array of visited nodes in the graph (used by Dijkstra's algorithm for shortest path computation) is not possible, which makes all sequential shortest path algorithms unimplementable.

### For details regarding the findings refer the report