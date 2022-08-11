package com.examples.optimization.joins.rdds

import com.examples.generator.DataGenerator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object I2ITransformation {

  val spark: SparkSession = SparkSession.builder()
    .appName("I2I Transformation")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  /*
    Science project
      Each metric has an identifiers, value.

    Return the smallest ("best") 10 metrics with (identifier, value)
   */

  //  DataGenerator.generateMetrics("src/main/resources/generated/metrics/metrics10m.txt", 10000000)

  val LIMIT = 10

  def readMetrics() = sc.textFile("src/main/resources/generated/metrics/metrics10m.txt")
    .map { line =>
      val tokens = line.split(" ")
      val name = tokens(0)
      val value = tokens(1)

      (name, value.toDouble)
    }

  // Strait forward solution
  def printTopMetrics() = {
    val sortedMetrics = readMetrics().sortBy(_._2).take(LIMIT)
    sortedMetrics.foreach(println)
  }

  def printTopMetricsI2I() = {

    val iterator2IteratorTransformations: Iterator[(String, Double)] => Iterator[(String, Double)] = (records: Iterator[(String, Double)]) => {

      implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)

      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
        limitedCollection.add(record)
        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }
      limitedCollection.iterator
    }

    val topMetrics = readMetrics()
      .mapPartitions(iterator2IteratorTransformations)
      .repartition(1)
      .mapPartitions(iterator2IteratorTransformations)

    val results = topMetrics.take(LIMIT)
    results.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(1000000)
  }

}
