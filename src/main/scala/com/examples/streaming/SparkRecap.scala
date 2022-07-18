package com.examples.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkRecap {

  // Entry point to the spark structure API
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Recap")
    .config("spark.master", "local")
    .getOrCreate()

  // Read a DF
  val cars: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  val guitarPlayersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  import spark.implicits._

  // Datasets -> typed distributed collections of objects
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  def main(args: Array[String]): Unit = {
    // actions
    cars.show()
    cars.printSchema()

    // select
    val usefulCarsData = cars.select(
      col("Name"),
      $"Year", // interpolate a string and obtain the column
      (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kg_v2")
    )

    // identical with select + expr
    val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")

    // filtering
    val europeanCars = cars.filter(col("Origin") =!= "USA")
    // equivalent using where
    val europeanCars2 = cars.where(col("Weight_in_lbs") =!= "USA")

    // aggregations
    val averageHP = cars.select(avg(col("Horsepower")).as("average_hp")) // sum, mean. stddev, min, max

    // grouping
    val countByOrigin = cars.groupBy(
      col("Origin")) // a relational grouped dataset on which you can call an aggregation function
      .count()

    // Joining

    var guitaristsBands = guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"))

    /** join types
     *   - inner: only the matching rows are kept
     *   - left / right/ full outer join
     *   - semi-joins / anti-join
     */

    // Datasets
    val guitarPlayerDS = guitarPlayersDF.as[GuitarPlayer]
    // once you get this DS you can apply map, filter, flatmap

    val guitarPlayerNamesDS: Dataset[String] = guitarPlayerDS.map(_.name)


    // Spark SQL
    cars.createOrReplaceTempView("cars") // create template which can be query using spark sql
    val usaCarsDF: DataFrame = spark.sql(
      """
        |select Name from cars where Origin = 'USA'
      """.stripMargin
    )

    // Low level API: RDD -> typed distributed collections of objects which don't have the SQL API

    // Creating RDD using spark context

    val sc = spark.sparkContext

    val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

    // on RDDs you can run functional operators
    val doublesRDD = numbersRDD.map(_ * 2)
    // Conversions functionality between RDD and DF

    // RDD -> DF
    val numbersDS = numbersRDD.toDF("number") // you'll lose the type info

    // RDD -> DS
    val numbersRDDv2 = spark.createDataset(numbersRDD)

    // DF -> RDD; Row are untyped structures
    val carsRDD: RDD[Row] = cars.rdd

  }

}
