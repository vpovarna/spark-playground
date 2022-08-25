package com.examples.performance

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkRecap {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  // Spark structure API is the SQL and dataframe SQL

  val cars: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars")


  // show dataframe
  cars.show()

  // print schema
  cars.printSchema()

  import spark.implicits._

  // select
  val usefulCarsData: DataFrame = cars.select(
    col("Name"), // creates a column object,
    $"Year", // needs spark implicits
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWeightDF: DataFrame = cars.selectExpr("Weight_in_lbs / 2.2") // identical to select + expr call

  // filtering
  val europeanCars: Dataset[Row] = cars.filter(col("Origin") =!= "USA")

  // aggregation
  val averageHP: DataFrame = cars.select(avg(col("Horsepower")).as("average_hp")) // at the end of this expression you'll get a single row
  // Other aggregation functions are: sum, mean, stddev, min, max

  // grouping
  val countByOrigin: DataFrame = cars
    .groupBy(col("Origin")) // a Relational GroupDataset
    .count()

  // joining
  val guitarPlayer: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bands: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")


  guitarPlayer.show()
  bands.show()


  val guitaristsBands: DataFrame = guitarPlayer.join(bands, guitarPlayer.col("band") === bands.col("id"))
  guitaristsBands.show()

  /*
      Join Types:
        - inner: only the matching rows are kept
        - left/ right / full outer join
        - semi / anti
   */

  // datasets == typed distributed collections of objects
  // You can convert the dataframe to a datasets by defining the type

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[String], band: Long)

  val guitarPlayerDS: Dataset[GuitarPlayer] = guitarPlayer.as[GuitarPlayer] // needs spark implicits

  guitarPlayerDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCarsDF: DataFrame = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin
  )

  americanCarsDF.show()

  // SQL + Dataframe + Datasets are Spark Structured API

  // low level API: RDDs:
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // Functional operators
  val doubles: RDD[Int] = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF: DataFrame = numbersRDD.toDF("number") // you loose the type info
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD) // keeps the type info and get the SQL capabilities

  // DS -> RDD
  val guitarPlayerRDD: RDD[GuitarPlayer] = guitarPlayerDS.rdd

  // DF -> RDD
  val carsRDD: RDD[Row] = cars.rdd

  def main(args: Array[String]): Unit = {

  }
}
