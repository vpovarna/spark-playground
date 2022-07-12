package com.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQL extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")


  val euCarsDF = carsDF.select(col("Name"))
    .where(col("Origin") === "USA")

  // run SQL using Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      """.stripMargin
  )

  americanCarsDF.show()

  // create an empty database
  spark.sql("create database demo")
  spark.sql("use demo")
  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  // To store the DB to another location, we need to add a new configuration in the spark session object

}
