package com.examples.optimization.joins.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.SizeEstimator

import java.lang

object BroadcastJoin {

  // Joining a large DF with a small DF
  // Small & powerful technique: broadcasting

  val spark: SparkSession = SparkSession.builder()
    .appName("broadcast joins")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("ERROR")
  val rows: RDD[Row] = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "seconds"),
    Row(3, "third")
  ))

  val rowSchema: StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // "small" table = usually a HashMap
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowSchema)

  println(SizeEstimator.estimate(lookupTable))

  // large table
  val table: Dataset[lang.Long] = spark.range(1, 1000000000) // default column is "id"

  val joined: DataFrame = table.join(lookupTable, "id")
  joined.explain()
  //  joined.show()

  // a smarter join
  val joinedDF: DataFrame = table.join(broadcast(lookupTable), "id")

  joinedDF.explain()
  // joinedDF.show()

  // auto-broadcast detection
  val bigTable = spark.range(1, 1000000000)
  val smallTable = spark.range(1, 1000) // size estimator by Spark - auto - broadcast
  val joinedNumbers = bigTable.join(smallTable, "id")

  joinedNumbers.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
