package com.examples.essentials.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object DataframeJoins extends App {

  // Joins are wide transformations !!! Involves shuffling

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")


  // join
  val guitarJoinCondition = guitaristDF.col("band") === bandsDF.col("id")
  // Inner join will combine the data from both dataframes based on join condition.
  guitaristDF.join(bandsDF, guitarJoinCondition, "inner").show()

  // outer join
  // left outer join == everything in the inner join + all the rows in the LEFT table, with nulls where the data is missing
  guitaristDF.join(bandsDF, guitarJoinCondition, "left_outer").show()

  // right outer
  // right outer join == everything in the inner join + all the rows in the RIGHT table
  guitaristDF.join(bandsDF, guitarJoinCondition, "right_outer").show()

  // full outer join
  // everything in the inner join + both in both tables
  guitaristDF.join(bandsDF, guitarJoinCondition, "outer").show()

  // semi-joins
  // inner join minus the data from the right dataframe
  guitaristDF.join(bandsDF, guitarJoinCondition, "left_semi").show()

  // anti-join
  // everything in the left dataframe for which don't satisfy the condition.
  guitaristDF.join(bandsDF, guitarJoinCondition, "left_anti").show()

  // things to bear in mind
  // columns with the same name

  // This will cause an error
  // guitaristDF.join(bandsDF, guitarJoinCondition, "inner")
  //  .select("id", "band")

  // option1 - rename the column on which are we joining
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option2 - drop the dupe column
  guitaristDF
    .join(bandsDF, guitarJoinCondition, "inner")
    .drop(bandsDF.col("id"))
    .show()

  // option 3 - rename the offending column and keep the data
  val bandsModDF =bandsDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId"))

  // Joins using Arrays (Dataframe as complex types). You can join based on complex expression
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()

}
