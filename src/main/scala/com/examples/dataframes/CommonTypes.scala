package com.examples.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Adding a value to DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  // Combining filters
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)

  // Evaluate the filter as column in the DF
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movies"))

  moviesWithGoodnessFlagsDF.where("good_movies") // because this is boolean the syntax is similar with where(col("good_movies") === "true")

  // Numbers
  val moviesAvgRatingDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // Correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // Corr is an action. This eager

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Capitalize the first letter of every word from the sentence
  carsDF.select(initcap(col("Name"))).show()
  // initcap, lower, upper

  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // contains can be done with a regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")

  // Replacing with regex
  vwDF.select(col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )
    .show()


  /**
   *  1. Filter the carsDF by a list of car names obtained by an API call.
   *     Versions:
   *    - contains
   *    - regex
   *
   */

  def getCarName: List[String] = List("volkswagen", "vw")

  val carFilterCondition: String = getCarName.map(_.toLowerCase()).mkString("|")
  carsDF.select(col("Name"),
    regexp_extract(col("Name"), carFilterCondition, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop(col("regex_extract"))
    .show()

  // Create a list of filters
  val carNameFilter = getCarName.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilter.fold(lit(false))((combineFilter, carNameFilter) => combineFilter or carNameFilter)
  carsDF.filter(bigFilter).show()

}
