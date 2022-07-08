package com.examples.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {

  // When defining a schema, use nullable=false only when you are sure that data doesn't have nulls.

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10) // coalesce will check the first column for null and if it's null will go to the next column
  )
//    .show()

  // check for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull)
//    .show()

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing rows where values are nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")) // add 0 in the IMDB_Rating and Rotten_Tomatoes_Rating columns which contains nulls.
  .show()

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // Complex operations

  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // similar with coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // returns null if both values are equals otherwise the first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)" // if (first != null) second else third
  )







}
