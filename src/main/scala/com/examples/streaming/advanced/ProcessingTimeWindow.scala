package com.examples.streaming.advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindow {

  val spark: SparkSession = SparkSession.builder()
    .appName("Processing Tine Window")
    .master("local[2]")
    .getOrCreate()


  def aggregateByProcessingTime() = {
    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value"), current_timestamp().as("processing_time"))
      .groupBy(window(col("processing_time"), "10 seconds").as("window"))
      .agg(sum(length(col("value")))) // counting characters every 10 secods by processing time

    linesDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

}
