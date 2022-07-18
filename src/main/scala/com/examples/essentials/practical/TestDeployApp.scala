package com.examples.essentials.practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Running the spark jar.
 *
 * ./spark-submit \
 *   --class com.examples.practical.TestDeployApp \
 *   --deploy-mode client \
 *   --master  spark://460f1384f661:7077 \
 *   --verbose \
 *   --supervise \
 *   /opt/spark-apps/spark-playground.jar /opt/spark-data/movies.json /opt/spark-data/goodComedies
 */
object TestDeployApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test deploy app")
      .getOrCreate()

    val moviesDF = spark.read
      .option("infer.schema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

}
