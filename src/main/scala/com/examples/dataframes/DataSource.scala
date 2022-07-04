package com.examples.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

// How to read multiple data formats in Spark.
object DataSource extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
   * Reading a DF:
   *      - format
   *      - schema (optional) or inferSchema = true
   *      - zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast") // mode decide what Spark should do when the rows doesn't match the defined schema.
    .load("src/main/resources/data/cars.json")

  // Modes are: failFast, dropMalformed, permissive (default)

  //!!! Loading dataframes and transforming dataframes are lazy until I call an action. Ex: show()


  // Another option is to use an option Map. By doing this you can create the options dynamically ar runtime
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map("mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))

  // Writing DFs:
  //   - format
  //   - saveMode: overwrite, append, ignore, errorIfExists
  //   - path
  //   - zero or more options

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dataFormat", "YYYY-MM-dd") // the dateFormat works only with provided schema
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // other compresion: bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // If the date format specified is incorrect, so if Spark failed parsing it will put null

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignores the first name and Spark will use the column names to validate against your schema.
    .option("sep", ",")
    .option("nullValue", "") // Nulls are transformed to empty string
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // Text Files
  val textDF = spark.read
    .text("src/main/resources/data/sampleTExtFile.txt")

  textDF.show()

  // reading from a remote database
  //  val employeesDF = spark.read
  //    .format("jdbc")
  //    .option("driver", "org.postgresql.Driver")
  //    .option("url", "jdbc:postgresql://localhost:5432/db")
  //    .option("user", "docker")
  //    .option("password", "docker")
  //    .option("dbtable", "public.employees")
  //    .load()

  /**
   * Read the moviesDF and write it as:
   *   - tab-separated values file
   *   - snappy Parquet
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .option("allowSingleQuotes", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.duplicate.parquet")

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")
}
