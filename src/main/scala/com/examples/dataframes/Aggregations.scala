package com.examples.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Spark aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Aggregations and grouping

  // 1. Counting.
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except nulls
  genresCountDF.show()

  moviesDF.selectExpr("count(Major_Genre)")

  // count all the rows in the DF
  val allMoviesDF = moviesDF.select(count("*")) // count all the rows and INCLUDE nulls
  allMoviesDF.show()

  // count distinct values
  moviesDF
    .select(countDistinct(col("Major_Genre")))
    .show()

  // For quick data analysis, we should do approximation
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // Statistic functions for min, max
  val minRating = moviesDF.select(min(col("IMDB_Rating")))
  // equivalent
  moviesDF.selectExpr("min(IMDB_Rating)")

  val sumUsGrossDF = moviesDF.select(sum(col("US_Gross")))
  // equivalent
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  // equivalent
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // Data science functions: mean, stddev
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF.
    groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating"))
  aggregationsByGenreDF.show()

  /**
   *  1. Sum all the profits of ALL the movies in the DF
   *     2. Count how many distinct directors we have
   *     3. Show the mean and standard deviation of US gross revenue for the movies.
   *     4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  // 1.
  val moviesUsGross = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum(col("Total_Gross")).as("Total_Gross_Sum"))
  moviesUsGross.show()

  // 2.
  val distinctDirectors = moviesDF.select(countDistinct(col("Director")))
  distinctDirectors.show()

  // 3.
  val moviesMeanStddevDF = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(
      mean(col("Total_Gross")),
      stddev(col("Total_Gross"))
    )

  moviesMeanStddevDF.show()

  moviesDF
    .select("Director", "IMDB_Rating", "US_Gross")
    .groupBy(col("Director"))
    .agg(
      sum(col("US_Gross")).as("US_Gross_Mean"),
      avg(col("IMDB_Rating")).as("IMDB_Rating_Mean")
    )
    .orderBy(col("IMDB_Rating_Mean").desc_nulls_last)
    .show()

}
