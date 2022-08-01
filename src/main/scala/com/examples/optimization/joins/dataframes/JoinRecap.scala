package com.examples.optimization.joins.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object JoinRecap {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Joins Recap")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("ERROR")

  val guitarPlayersDF: DataFrame = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF: DataFrame = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/bands.json")

  val guitarsDF: DataFrame = spark.read
    .option("infer.schema", "true")
    .json("src/main/resources/data/guitars.json")

  // inner join

  val joinCondition: Column = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF: DataFrame = guitarPlayersDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // 1. left outer join = everything in the join + all the rows in the left table with nulls that are not passing the conditions from the right table

  guitarsDF.join(bandsDF, joinCondition, "left_outer")

  // 2. right outer join = everything in the join + all the rows in the right table with nulls that are not passing the conditions from the left table
  guitarsDF.join(bandsDF, joinCondition, "right_outer")

  // 3. full outer joins = everything in the left_outer + right_outer
  guitarsDF.join(bandsDF, joinCondition, "full_outer")

  // semi joins = everything in the left DF for witch there is a row a row in the right DF satisfying the confitions
  guitarsDF.join(bandsDF, joinCondition, "left_semi")

  // anti joins = everything in the left DF for which there is not a row in the right DF satisfying the condition
  guitarsDF.join(bandsDF, joinCondition, "left_anti")

  // cross join = combining everything from the left table with everything from the right table
  // careful with outer joins with non-unique keys


  // RDD Joins
  val colorsScores = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 5),
    ("yellow", 2),
    ("orange", 3),
    ("cyan", 0)
  )

  // RDDs of pairs can be join by key, where the first item in he tuple is the key
  val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorsScores)

  val text = "The sky is blue, but the orange pale sun turns from yellow to red"
  val words: Array[(String, Int)] = text.split(" ")
    .map(_.toLowerCase)
    .map(word => (word, 1)) // standard technique for counting words with RDDs

  val wordsRDD: RDD[(String, Int)] = sc.parallelize(words)
    .reduceByKey(_ + _) // count the occurrence of every words

  // join wordsRDD with colorsRDD
  val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD) // implies join type inner

  def main(args: Array[String]): Unit = {
    guitarPlayersDF.show()
    bandsDF.show()
    guitarsDF.show()

    guitaristsBandsDF.show()
  }

}
