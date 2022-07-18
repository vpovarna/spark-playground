package com.examples.essentials.dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameBasic extends App {

  // creating the spark session
  val spark = SparkSession.builder()
    .appName("DataFrames Basic")
    .config("spark.master", "local[*]")
    .getOrCreate()


  // reading a DF
  val firstDF: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing the DF
  firstDF.show()
  // printing the schema
  firstDF.printSchema()

  // A dataframe is a schema a distribute collections of rows conforming to the schema.

  // println will show
  firstDF.take(10).foreach(println)

  // Spark types
  val longType = LongType
  val stringType = StringType

  // schema
  // For complicated schemas, we can use StructType.
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // Obtain the schema of an existing dataset
  var carsDFSchema = firstDF.schema
  println(carsDFSchema)

  /**
   *   !! In production we should we not read data with inferSchema = true
   */

  // Read a dataframe with your own schema
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsDF.show()

  // Create a row programmatically
  val myRow = Row("8.5,8,390.0,190,15.0,amc ambassador dpl,USA,3850,1970-01-01")

  // Create DF from a sequence of tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // Note: DFs have schemas, rows do not

  // Create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Wight", "Acceleration", "Year", "Origin")

  manualCarsDF.printSchema() // columns name are: _1, _2, _3, etc
  manualCarsDFWithImplicits.printSchema() // using this method, the strings from the toDF method will be used as columns

  /*
      Exercises:
        1. Create a manual DF describing smartphones
              - make
              - model
              - screen dimension
              - camera megapixels
        2. Read another file from the data folder. Movies dataset. Print schema and count the number of rows.
   */


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  println(s"The number of movies from the Movies DF: ${moviesDF.count()}")

  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "Iphone X", "IOS", 13),
    ("Nokia", "3310", "The Best", 0),
  )

  val smartPhonesDF: DataFrame = smartphones.toDF("Make", "Model", "Platform", "Camera MPX")
  smartPhonesDF.show()
}
