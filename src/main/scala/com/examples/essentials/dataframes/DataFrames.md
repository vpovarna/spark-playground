## DataFrames

DataFrames are distributed collections of rows conforming to a schema = list describing the column names and types.
The types are known to Spark and not at compile type.
Rows have the same structure

DataFrames are immutable. Now DFs are created using transformations

Transformations:
-> narrow: one input partitions will contribute to at most one output partitions
-> wide: one input partitions will create multiple output partitions.

Shuffle: Data exchange between cluster nodes.
occurs in wide transformations.
massive perf impact

Lazy evaluation == sparks wait until the last moment to execute the DF transformations
Spark will create a graph of transformations before running the graph.
logical plan = DF dependency graph + narrow/wide transformations sequence
physical plan = optimized sequence of steps for nodes in the cluster.

Transformations vs Actions
transformations describes how new DFs are obtained
actions actually start executing Spark code.