package com.examples.streaming.strucured

import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object StreamingDataFrames {
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Stream")
    .config("spark.master", "local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def readFromSocket(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Transformations can vbe added between reading and consuming data
    val shortLines = lines.filter(length(col("value")) <= 5)

    // check if the dataframe is static or not
    println(shortLines.isStreaming)

    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def readFromFiles(): Unit = {

    val stockSchema = StructType(Array(
      StructField("company", StringType),
      StructField("date", DateType),
      StructField("value", DoubleType)
    ))

    val stocksDF = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stockSchema) // this needs to be specified
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
//        Trigger.ProcessingTime(2 seconds) // combine data in 2 sec chunks
//        Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2 seconds) // experimental, every 2 seconds create a batch with whatever it receives in the specified interval
    )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromSocket()
//    readFromFiles()
//    demoTriggers()
  }
}
