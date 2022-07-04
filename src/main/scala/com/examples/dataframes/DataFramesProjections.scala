package com.examples.dataframes

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DataFramesProjections extends App {

  val spark = SparkSession.builder()
    .appName("Data Frame Projections")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Get column from DF
  val firstColumn: Column = carsDF.col("Name")

  // selecting == projection
  val carsNameDF: DataFrame = carsDF.select(firstColumn)
  carsNameDF.show()

  // various ways to select columns from dataframes

  import spark.implicits._

  carsDF.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower",
    expr("Origin") // Expression
  )

  // select with plain column names
  val anotherDF = carsDF.select("Name", "Year")

  // selecting is a narrow transformation. Every partition has a corresponding result partition

  // EXPRESSIONS
  val simpleExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = carsDF.col("Weight_in_lbs") / 2.2 // this is  a column

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightDF.show()

  // In practice we are doing select with various expr statements. For this we can use selectExpr directly
  val carsWithSelectExpr = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a new column
  val carsWithKg3 = carsDF.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
  carsWithKg3.show()

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  // equivalent with where
  val europeanCarsDFv2 = carsDF.where(col("Origin") =!= "USA")
  // equivalent with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filter
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  // chain filter in a single filter
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // if dataframe in question have the same schema.

  // distinct
  val allCountryDF = carsDF.select(col("Origin")).distinct()
  allCountryDF.show()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select("Title", "Release_Date")
    .show()

  moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross")
    .show()

    moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
      .withColumn("Total_profit", col("Worldwide_Gross") + col("US_Gross"))
      .show()

  moviesDF
    .select("Title", "Major_Genre", "IMDB_Rating")
    .filter(col("Major_Genre") === "Comedy")
    .filter(col("IMDB_Rating") > 6)
    .show()

  moviesDF.select("Title", "Major_Genre", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    .show()
}
