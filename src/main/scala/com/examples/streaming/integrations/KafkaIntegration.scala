package com.examples.streaming.integrations

import com.examples.streaming.util.Schema.carsSchema
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaIntegration {

  val spark: SparkSession = SparkSession.builder()
    .appName("Integration Kafka")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  def readFromKafka(): Unit = {

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm") // we can subscribe on multiple topics in the value string
      .load()

    // print df to console ; start & wait for termination
    kafkaDF
      .select(expr("cast(value as string) as actualValue"),
        col("topic"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    // for a DF to be kafka compatible we need key and value
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    val query: StreamingQuery = carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // mandatory option; used for kafka caching
      .start()
    query
      .awaitTermination()
  }

  // Write the hole cars Data Structure as string
  def writeCarsToKafka(): Unit = {
    import spark.implicits._

    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF
      .select(
        col("Name").as("key"),
        to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
      )

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToKafka()
  }

}
