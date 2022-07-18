package com.examples.streaming.strucured

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregation {
  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming Aggregation")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val lines: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  def streamingCount(): Unit = {
    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")
    // aggregation with distinct are not supported
    // otherwise spark will need to keep track of everything.

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update are not supported on aggregation without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregation(aggregation: Column => Column): Unit = {
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggregation(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // counting occurrence of the "name" value
  def groupNames(): Unit = {
    val names = lines.select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    numericalAggregation(max)
    groupNames()
  }
}
