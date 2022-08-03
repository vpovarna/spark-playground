package com.examples.optimization.joins.rdds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object BroadcastJoin {

  val spark = SparkSession.builder()
    .appName("RDDs Broadcast Joins")
    .master("local[*]")
    .getOrCreate()

  val random = new Random()
  val sc = spark.sparkContext

  val order = sc.parallelize(List((1, "gold medal"), (2, "silver medal"), (3, "bronze medal")))
  val leaderboard = sc.parallelize(1 to 1000000).map((_, random.alphanumeric.take(8).mkString))

  val winners = leaderboard.join(order)


  // Broadcast join.
  // 1. Transform order rdd to Map and collect it into driver memory
  val medalsMap = order.collectAsMap()

  // 2. Broadcast the map to the executors
  sc.broadcast(medalsMap)

  // 3. for every iterator / data withing every partition, check if the position is in the broadcast map
  val improvedWinners: RDD[(String, String)] = leaderboard.mapPartitions {
    // this function is executed individual by each executors
    iterator: Iterator[(Int, String)] =>
      iterator.flatMap { record: (Int, String) => {
        val (position, name) = record
        medalsMap.get(position) match {
          case Some(prize) => Seq((name, prize))
          case None => Seq.empty
        }
      }
      }
  }

  def main(args: Array[String]): Unit = {
//    winners.foreach(println)
    improvedWinners.foreach(println)
  }

}
