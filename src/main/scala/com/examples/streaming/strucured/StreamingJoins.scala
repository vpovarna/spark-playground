package com.examples.streaming.strucured

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingJoins {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Streaming Join")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val guitarPlayers: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitars: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val bands: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val joinCondition: Column = guitarPlayers.col("band") === bands.col("id")
  // static dataframe join
  val guitaristsBands: DataFrame = guitarPlayers.join(bands, joinCondition, "inner")

  val bandsSchema: StructType = bands.schema
  val guitarPlayerSchema: StructType = guitarPlayers.schema

  def joinStreamWithStatic(): Unit = {

    val streamsBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr(
        "band.id as id",
        "band.name as name",
        "band.hometown as hometown",
        "band.year as year"
      )

    // Join happens per batch
    val streamedBandsGuitaristsDF = streamsBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamsBandsDF.col("id"), "inner")

    /**
     * Restricted Join
     *   Stream join with static RIGHT -> Right Outer join and full outer join and right semi and right anto joins are not permitted.
     *   Static Left with stream join -> Left Outer join and full outer join and left semi and left anto joins are not permitted.
     */

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // Stream vs stream join is allowed since spark 2.3
  def joinStreamWithStream: Unit = {
    val streamsBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr(
        "band.id as id",
        "band.name as name",
        "band.hometown as hometown",
        "band.year as year"
      )

    val streamsGuitaristDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 123)
      .load()
      .select(from_json(col("value"), guitarPlayerSchema).as("guitarPlayer"))
      .selectExpr(
        "guitarPlayer.id as id",
        "guitarPlayer.name as name",
        "guitarPlayer.guitars as guitars",
        "guitarPlayer.band as band"
      )

    val streamedJoin = streamsBandsDF.join(streamsGuitaristDF, streamsGuitaristDF.col("band") === streamsBandsDF.col("id"), "inner")

    /**
     *  - inner join are supported
     *  - left / right outer joins ARE supported but must have watermarks
     *  - full outer joins are not supported.
     */

    streamedJoin.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    guitarPlayers.show()

    joinStreamWithStream
  }
}
