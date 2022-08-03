package com.examples.optimization.joins.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array_contains, upper}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnPruning {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Pruning")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("ERROR")

  val guitarsDF: DataFrame = spark.read
    .option("InferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF: DataFrame = spark.read
    .option("InferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF: DataFrame = spark.read
    .option("InferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val smallJoinCondition: Column = guitarPlayersDF.col("band") === bandsDF.col("id")

  val guitaristBandsDF: DataFrame = guitarPlayersDF.join(bandsDF, smallJoinCondition, "inner")
  guitaristBandsDF.explain()

  // left_anti = select everything from the left DF for which there are no rows in the right DF
  val guitaristWithoutBandsDF: DataFrame = guitaristBandsDF.join(bandsDF, smallJoinCondition, "left_anti")
  guitaristWithoutBandsDF.explain()

  // project and filter push down
  val namesDF: DataFrame = guitaristWithoutBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"))
  namesDF.explain()

  val rockDF: DataFrame = guitarPlayersDF
    .join(bandsDF, smallJoinCondition)
    .join(guitarsDF, array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id")))

  val essentialDF: DataFrame = rockDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")))

  //  == Physical Plan ==
  //  *(3) Project [name#25, name#39, upper(make#9) AS upper(make)#163]
  //  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#8L)
  //  :- *(2) Project [guitars#23, name#25, name#39]
  //  :  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildRight
  //    :     :- *(2) Project [band#22L, guitars#23, name#25]
  //  :     :  +- *(2) Filter isnotnull(band#22L)
  //  :     :     +- FileScan json [band#22L,guitars#23,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/Users/povarna/Documents/github/spark-playground/src/main/resources/data/g..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
  //  :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#186]
  //  :        +- *(1) Project [id#38L, name#39]
  //  :           +- *(1) Filter isnotnull(id#38L)
  //  :              +- FileScan json [id#38L,name#39] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/Users/povarna/Documents/github/spark-playground/src/main/resources/data/b..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
  //    +- BroadcastExchange IdentityBroadcastMode, [id=#176]
  //      +- FileScan json [id#8L,make#9] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/Users/povarna/Documents/github/spark-playground/src/main/resources/data/g..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>

  // If the join table result is larger then the initial table, do the upper function before; otherwise do it at the end.
  essentialDF.explain()

  def main(args: Array[String]): Unit = {

  }
}
