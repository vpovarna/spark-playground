package com.examples.optimization

import com.examples.streaming.SparkRecap.cars
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkRecap extends App {
  // Entry point to the spark structure API: SQL, dataset
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Recap")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF: DataFrame = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/cars")

  // action
  carsDF.show()

  import spark.implicits._

  // select
  val usefulCarsData = carsDF.select(
    col("Name"), // column object
    $"Year", // another column object == needs spark implicits
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_v2")
  )

  // Equivalent with select + expr
  val carsWights = carsDF.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCars = carsDF.filter(col("Origin") =!= "USA")

  // agg
  val averageHP = carsDF.select(avg(col("Horsepower")).as("average_hp")) //other aggregation functions: sum. mean , stddev, min, max

  // grouping
  val carsByOrigin = carsDF
    .groupBy(col("Origin"))
    .count()


  // joining
  val guitarists = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bands = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/bands.json")


  val guitaristsBands = guitarists.join(bands, guitarists.col("band") === bands.col("id"))

  // Join Types:
  //    - inner: only the matching rows are kept
  //    - left / right / full outer join
  //    - semi / anti

  // datasets

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  val guitarPlayerDS = guitarists.as[GuitarPlayer] // needs spark implicits
  guitarPlayerDS.map(_.name)

  carsDF.createOrReplaceTempView("cars")

  val americansCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin
  )

  // low level API: RDDs
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")

  // Using spark context we can create RDDs

  val numbers:RDD[Int] = sc.parallelize(1 to 1000000)
  val doubles: RDD[Int] = numbers.map(_ * 2)

  // RDD -> to DF
  val numbersDF = numbers.toDF("number") // you lose the type info but you get the sql capabilities
  val numbersDS = spark.createDataset(numbers)

  // DS -> RDD
  val guitarPlayersRDD = guitarists.rdd
  val carsRDD = cars.rdd // this is a RDD of rows. Which is a generic type

}
