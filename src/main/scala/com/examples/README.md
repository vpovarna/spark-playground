## Spark Execution Terminology

A job has multiple stages and a stage has multiple tasks.

 - stage = a set of computation between shuffle
 - task = a unit of computation per partition
 - shuffle = exchange of data between spark nodes.

Getting the number of partition:
```
scala> rdd.getNumPartitions
```
 - DAG = graph of RDD dependencies

Repartition rdd
```
scala> val rdd3 = rdd1.repartition(25)
scala> rdd3.count
```

Repartition will create a new stage.

You can get the physical plan by running: 
```
scala> ds1.explain
```

## Anatomy of a Cluster

Spark cluster manager
 - one node manages the state of the cluster
 - the others do the work
 - communicate via driver / worker process

Cluster Managers: Standalone, YARN, Mesos, K8S

Spark driver:
 - manages the state of the stages / tasks of the application
 - interface with the cluster manager

Spark executors:
 - run the task assign by the spark driver
 - report their state and results to the driver
 
Execution modes:
 - cluster = spark driver is lunched on the worker node.
 - client = spark driver stays on the client and the spark cluster manager will create the executors so the driver will then communicate to them
 - local

## Spark Architecture

Low level API: DStream, RDDs, Distributed Variables
Higher Level Structured API: DataFrames, Datasets, Spark SQL
Applications: Streaming, ML, GraphX, Other Libraries

**Stream processing** = computation is done when new data comes in. No definitive end of incoming data.

Pros / Cons
 Pros:
  - Much lower latency than batch processing
  - Greater performance / efficiency (especially with incremental data)

 Cons:
  - Maintaining state and order for incoming data
  - Exactly once processing in the context of machine failure
  - Responding to events ar low latency
  - Updating business logic at runtime

Spark Streaming operates on micro-batches. 

Input Source:
 - Kafka, Flume
 - A distributed file system
 - Sockets

Output Source: almost all available formats

Streaming I/O
 - append = only add new records
 - update = modify records in place
 - complete = rewrite everything

Trigger = when the new data is written.

Aggregation in spark streaming works at the micro batch level, only when the batch is created.
Append output mode is not supported on aggregation without watermarks

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