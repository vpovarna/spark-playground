##Spark Concepts

### Spark Layered Architecture

|Streaming  | ML  | GraphX  |  Other Libraries|<br />
|Dataframes     |    Datasets    |    SparkSQL|<br />
|   RDD           |    Distributed Variables  |<br />
|    Cluster Manager e.g: YARN                |<br />
|    Storage Layer e.g: HDFS                  |<br />


Storage will access files, HDFS, S3, databases

Spark supports 4 cluster manager:
- standalone (bundled with Spark)
- YARN
- Mesos
- Kubernetes

Spark Core:
- RDDs
- Dataframes & SQL; both have a lot of optimizations out of the box

High Level Libs: 
- Streaming
- ML
- GraphX

Spark Architecture

A cluster of worker nodes performs the work / data processing

Executors = worker logical node (JVM)
  - performs work from a single application
  - usually more than one per application
  - launched in JVM containers with their own memory / CPU resource
  - can be 0 or more deployed on the same physical machine

Driver = spark application main JVM
  - one per application
  - starts the application and sends work to the executors

Cluster manages the deployment of the executors
  - driver requests executors & resources from the cluster manager

For performance:
  - driver should be close to the worker node (same physical rack or at least same LAN)
  - worker nodes close to each other - otherwise shuffle operation are expensive

#### RDDs
Distributed typed collections of JVM objects
Pros: can be highly customized
  - distribution can be controlled
  - order elements can be controlled
  - arbitrary computation hard/ impossible to express with SQL 
Cons: hard to work with
  - for complex operations, needs to know the internals of Spark
  - poor APIs for quick data processing

#### DataFrames
High level distributed data structure
Contains Rows
  - have a schema
  - have additional API for querying
  - supports SQL directly on top of them
  - generate RDDs after Spark SQL planning & optimizing
Pros:
  - easy to work, supports SQL
  - already heavily optimized
Cons:
  - type-unsafe
  - unable to compute everything
  - hard to optimize further

#### Dataset
Distributed typed collections of JVM objects
 - supports sql functions of dataframe
 - supports functional operations like RDDs

Dataframe = Dataset[Row]

Pros:
  - easy to work with, supports both SQL and functional programming
  - type-safe
  - some Spark optimizations out of the box
Cons:
  - memory and CPU expensive to create JVM objects
  - unable to optimize lambdas

Performance Tips:
  - Use DataFrames most of the time
    - express almost anything with SQL
    - Spark already optimizes most SQL functions 
  - Use RDDs only in custom processing
  - Do not switch types
    - DFs to RDD[YourType] or Dataset[YourType] is expensive

#### Computing Anything
Lazy evaluation:
   - Spark waits until the moment to execute the DF / RDD transformations

Planning
   - Spark compiles DF/SQL transformations to RDD transformations (if necessary)
   - Spark compile RDD transformations into a graph before running any code
   - logical plan = RDD dependency graph + narrow / wide transformations sequence
   - physical plan = optimized sequence of steps for nodes in the cluster
Transformations returns another DF or RDD
Action will return something else: unit, number, array, etc

An action will trigger spark job. 
A job is split into stages:
  - each stage is dependent on the stage before it
  - a stage must fully complete before the next stage can start
  - for performance, usually minimize the number of stages

A stage has tasks.
  - task = the smallest unit of work
  - tasks are run by executors

An RDD / Dataframe / Datasets has partitions. Data is split between chunks. These chunks will be sent to executors

#### Concepts Relationships
App decomposition
- 1 job = 1 or more stages
- 1 stage = 1 or more tasks

Tasks & Executors
- 1 task is run by 1 executors
- each executor can run 0 or more tasks. Tasks are running in sequence

Task is the fundamental unit of work. It can process one partitions.

Partitions & Tasks
- one partition is processed by on one task by one executor
- each executor can load 0 or more partitions in memory on disk

Executors & Nodes
- executors = JVM or physical node
- each physical node can have one or more executors

#### Narrow vs Wide dependencies

Narrow dependencies:
  - one input (parent) partition influences a single output (child) partition
  - fast to compute
  - examples: map, flatMap, filter, projections

Wide dependencies:
  - one inout partition influence more than one output partitions
  - involving a shuffle = data transfer between Spark executors
  - are costly to compute
  - examples: grouping, joining, sorting

#### Starting a Spark application
/spark/bin/spark-submit \
  --class foundation.TestApp
  --master spark://(dockerId):7077 \ 
  --deploy-mode client \
  --verbose \
  --supervise \
  spark-playground.jar data/movies.json data/goodMovies

Other command line args:
  -executor-memory = allocate a certain amount of RAM/executor
  -driver-memory = allocate a certain amount of RAM to the driver
  -jars = additional JAR libraries for Spark to have access to
  -packages = add additional libraries as Maven coordinate
  -conf (configName) (configValue) = other configuration supplied

Most of the time, application will be submited in cluster mode. 

#### Optimizing DataFame Transformations

If DFs or RDDs don't have a known partitioner, a shuffle is needed
Shuffled Join (very expensive)
 - data transfer overhead
 - potential OOMs
 - limited parallelism

Co-located RDDs
 - have the same partitioner
 - reside in the same physical location in memory (on the same executor)
 - can be joined without network transfer

Co-partitioned RDDs
  - have the sane partitioner
  - may be on different executors
  - will be joined with network traffic
    - although much less than without the partitioning information.

Optimised join:
 - Shuffle only one RDD by forcing the repartition.
 - If both RDD have the same partitioner, then there is no shuffle. (co-partitioned)
 - Data is partitioned and it's loaded in memory. (co-located)


#### Joins Optimization

When we want to join a large dataset with a small dataset we can optimize the join using a broadcast join 
Spark has added auto-broadcast detection. 
  - size estimator by Spark - auto - broadcast
  - it can be configured in spark session using the spark.sql.autoBroadcastJoinThreshold 
    - spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 30) -> adding the size of the range for auto-broadcast
    - spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) -> deactivate auto broadcast

Broadcast Join: 
  Share the smaller DF/RDD across all executors
    - tyne overhead
    - all the operations are done in memory

  Pros:
    - shuffles avoided
    - insane speed
  Risk:
    - not enough driver memory
    - if smaller DF is quite big -> OOMing executors
  
  Broadcast can be done automatically by spark. Finds the DF smaller than threshold.