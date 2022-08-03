package com.examples.optimization.joins.dataframes

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Bucketing {

  val spark: SparkSession = SparkSession.builder()
    .appName("Bucketing")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  val large: Dataset[Row] = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small: Dataset[Row] = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined: DataFrame = large.join(small, "id")
  joined.explain()

  // Bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small") // bucketing ans saving almost as expensive as a regular shuffle

  //   bucketing and save it's similar wih hash partitioning
  val bucketedLarge: DataFrame = spark.table("bucketed_large")
  val bucketedSmall: DataFrame = spark.table("bucketed_small")

  val bucketingJoin: DataFrame = bucketedSmall.join(bucketedLarge, "id")

  bucketingJoin.explain()

  // bucketing for groups
  val flightsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights")
    .repartition(2)

  val mostDelayed: Dataset[Row] = flightsDF.filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  mostDelayed.explain()

  /*
      == Physical Plan ==
      *(4) Sort [avg(arrdelay)#60 DESC NULLS LAST], true, 0
      +- Exchange rangepartitioning(avg(arrdelay)#60 DESC NULLS LAST, 200), true, [id=#204]
         +- *(3) HashAggregate(keys=[origin#34, dest#31, carrier#25], functions=[avg(arrdelay#24)])
            +- Exchange hashpartitioning(origin#34, dest#31, carrier#25, 200), true, [id=#200]
               +- *(2) HashAggregate(keys=[origin#34, dest#31, carrier#25], functions=[partial_avg(arrdelay#24)])
                  +- Exchange RoundRobinPartitioning(2), false, [id=#196]
                     +- *(1) Project [arrdelay#24, carrier#25, dest#31, origin#34]
                        +- *(1) Filter (((isnotnull(origin#34) AND isnotnull(arrdelay#24)) AND (origin#34 = DEN)) AND (arrdelay#24 > 1.0))
                           +- FileScan json [arrdelay#24,carrier#25,dest#31,origin#34] Batched: false, DataFilters: [isnotnull(origin#34), isnotnull(arrdelay#24), (origin#34 = DEN), (arrdelay#24 > 1.0)], Format: JSON, Location: InMemoryFileIndex[file:/Users/povarna/Documents/github/spark-playground/src/main/resources/data/f..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), IsNotNull(arrdelay), EqualTo(origin,DEN), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>
   */

  flightsDF.write
    .partitionBy("origin")
    .bucketBy(4, "dest", "carrier")
    .saveAsTable("flights_bucketed") // will take just as long as a shuffle


  val flightsBucketed: DataFrame = spark.table("flights_bucketed")

  val mostDelayed2: Dataset[Row] = flightsBucketed.filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  mostDelayed2.explain()
  /*
      == Physical Plan ==
      *(2) Sort [avg(arrdelay)#148 DESC NULLS LAST], true, 0
      +- Exchange rangepartitioning(avg(arrdelay)#148 DESC NULLS LAST, 200), true, [id=#165]
         +- *(1) HashAggregate(keys=[origin#122, dest#119, carrier#113], functions=[avg(arrdelay#112)])
            +- *(1) HashAggregate(keys=[origin#122, dest#119, carrier#113], functions=[partial_avg(arrdelay#112)])
               +- *(1) Project [arrdelay#112, carrier#113, dest#119, origin#122]
                  +- *(1) Filter (isnotnull(arrdelay#112) AND (arrdelay#112 > 1.0))
                     +- *(1) ColumnarToRow
                        +- FileScan parquet default.flights_bucketed[arrdelay#112,carrier#113,dest#119,origin#122] Batched: true, DataFilters: [isnotnull(arrdelay#112), (arrdelay#112 > 1.0)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/povarna/Documents/github/spark-playground/spark-warehouse/flights_b..., PartitionFilters: [isnotnull(origin#122), (origin#122 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4
   */

  /**
   * Bucket pruning = when you need a subset of data.
   *
   */

  val the10 = bucketedLarge.filter($"id" === 10)
  the10.show()
  the10.explain()

  def main(args: Array[String]): Unit = {

  }
}
