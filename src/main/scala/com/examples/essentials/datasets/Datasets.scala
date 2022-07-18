package com.examples.essentials.datasets

import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // now we can apply any scala functions
  numbersDS.filter(_ < 100)

  // For complex type we'll define a case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String)

  def readDataframe(fileName: String) = {
    spark.read.option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }

  val carsDF = readDataframe("cars.json")

  import spark.implicits._

  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  val carNamesDS: Dataset[String] = carsDS.map(car => car.Name.toLowerCase())

  // In order to allow nulls in the DS, add Options
  carNamesDS.show()

  /** Exercises
   *  1. Count how many cars we have
   *     2. Count how many powerful cars we have
   *     3. Compute the AVG HP for the entire DS
   */

  val nrOfCars: Long = carsDS.count()
  println(s"The number of cars in the dataset is: $nrOfCars")

  val powerfulCarsNr = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(s"The number of powerful cars is: ${powerfulCarsNr}")

  val avgHP = carsDS.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _) / nrOfCars
  val avgHPv2 = carsDS.select(avg(col("Horsepower")))
  println(s"Avg HP is: $avgHP")

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType:String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Bands(id: Long, name: String, hometown: String, year: Long)

  val guitarDS = readDataframe("guitars.json").as[Guitar]
  val guitarPlayerDS = readDataframe("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDataframe("bands.json").as[Bands]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Bands)] = guitarPlayerDS
    .joinWith(bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show

  /** Exercises:
   *    - join the guitarDS and guitarPlayerDS, in an outer join type
   *       (hint: use array contains)
   */

  guitarDS.joinWith(guitarPlayerDS, array_contains(guitarPlayerDS.col("guitars"), guitarDS.col("id")), "outer")
    .show()


  // Grouping
  val carsGroupByOrigin: Dataset[(String, Long)] = carsDS.groupByKey(_.Origin)
    .count()

  carsGroupByOrigin.show()

  // Joins and Groups are WIDE transformations. They involves shuffle
}
