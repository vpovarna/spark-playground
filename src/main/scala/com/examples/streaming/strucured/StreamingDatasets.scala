package com.examples.streaming.strucured

import com.examples.streaming.util.Car
import com.examples.streaming.util.Schema.carsSchema
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDatasets {
  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming datasets")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // include encoders for DF -> DS transformation

  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // manually encoder
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      .as[Car](carEncoder)
  }

  val carsDS: Dataset[Car] = readCars()


  def showCarName: Unit = {
    val carNameDF = carsDS.select(col("Name")) // this is a dataframe. We are losing the type info
    // collection transformation maintains the type info
    val carNamesAlt = carsDS.map(_.Name) // dataset of String
    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
   * Count how many Powerful cars we have in the DS (HP>140)
   * Average HP for the entire dataset (use the complete output mode)
   * Count the cars by the origin field
   */

  // Ex 1
  def countPowerfulCars = {
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140L)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // Ex 2
  // By using the compelte we're calculating the avg for the entire data which goes into the topic.
  def calculateAverageHP = {
    carsDS
      .agg(avg("Horsepower").as("avg_horsepower"))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Ex3
  def showCarsByOrigin: Unit = {
    carsDS
      .groupByKey(_.Origin)
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    countPowerfulCars
  }

}
