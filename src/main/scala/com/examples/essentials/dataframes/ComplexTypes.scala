package com.examples.essentials.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("ComplexType")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/movies.json")

  // Operate the dates if they are read as String

  val moviesWithReleaseDate = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-YY").as("Actual_Release")) // conversion

  val moviesDateDF = moviesWithReleaseDate
    .withColumn("Today", current_date()) // returns today
    .withColumn("Right_Now", current_timestamp()) // returns seconds
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date difference

  //  moviesDateDF.show(truncate = false)

  // If spark can't parse the date will return null

  val nullsReleaseDateMoviesDF = moviesWithReleaseDate.select("*").where(col("Actual_Release").isNull)
  //  nullsReleaseDateMoviesDF.show(truncate = false)

  // date_add = add a number of days
  // date_sub = subtract a number of days

  /**
   * 1. How do we deal with multiple formats?
   * 2. Parse the stocks.csv
   */

  // 1
  // One solution is to parse the DF multiple times and then union the small DFs


  // 2
  val stocksDF = spark.read
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select(
    col("symbol"),
    to_date(col("date"), "MMM dd YYYY").as("parsed_date"),
    col("price")
  )
    .show(truncate = false)

  // Structures
  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")) // creates an array
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
  //    .show(truncate = false)

  // 2 - with expression
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
  //    .show(truncate = false)

  // Arrays
  val moviesWithWordsDF = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // the items in the column Title will be split by " " or "," and will return an Array

  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
    .show()
}

