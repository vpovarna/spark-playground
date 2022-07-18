package com.examples.streaming.util

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object Schema {

  val stockSchema: StructType = StructType(Array(
    StructField("company", StringType),
    StructField("date", DateType),
    StructField("value", DoubleType)
  ))

  // schema
  // For complicated schemas, we can use StructType.
  val carsSchema: StructType = StructType(Array(
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

}
