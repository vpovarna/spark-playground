package com.examples.streaming

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EventsCount extends App {

  val spark = SparkSession.builder()
    .appName("Spark Streaming")
    .config("spark.master", "local")
    .getOrCreate()


  val eventsCountDF: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "trending-count-topic")
    .option("startingOffsets", "earliest")
    .load()

  eventsCountDF
    .select(col("topic"), expr("cast(value as string) as actualValue"))
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
}
