package com.examples.streaming.advanced

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {

  val spark: SparkSession = SparkSession.builder()
    .appName("Event time window")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  val onlinePurchaseSchema: StructType = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket(): DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesBySlidingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // Struct column: has fields start / end
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesByTumblingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // Tumbling window: sliding duration = window duration. Windows don't overlap
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Exercises:
   *   1. Show the best selling product of every day
   *   2. Show the best selling product of every 24 hours updated every hour.
   *
   */

    def bestSellingProduct(): Unit = {
      val purchaseDF = readPurchasesFromFile()

      val windowPurchaseCount = purchaseDF
        .groupBy(col("item"), window(col("time"), "1 day").as("day"))
        .agg(sum(col("quantity")).as("total_quantity"))
        .select(
          col("day").getField("start").as("start"),
          col("day").getField("end").as("end"),
          col("item"),
          col("total_quantity")
        )
        .orderBy(col("day"), col("total_quantity").desc)

      windowPurchaseCount.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
    }

  def bestSlidingSellingProductEvery24h(): Unit = {
    val purchaseDF = readPurchasesFromFile()

    val windowPurchaseCount = purchaseDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))

      .agg(sum(col("quantity")).as("total_quantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("total_quantity")
      )
      .orderBy(col("start"), col("total_quantity").desc)

    windowPurchaseCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    bestSellingProduct()
  }

}
