package com.examples.dataframes

import org.apache.spark.sql.SparkSession

object DataframeJoins extends App {

  // Joins are wide transformations !!! Involves shuffling

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


}
