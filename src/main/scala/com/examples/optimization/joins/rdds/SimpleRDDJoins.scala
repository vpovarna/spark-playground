package com.examples.optimization.joins.rdds

import com.examples.generator.DataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.SparkSession

object SimpleRDDJoins {

  val spark: SparkSession = SparkSession.builder()
    .appName("RDD Joins")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val rootFolder = "src/main/resources/data/generated/examData"

//  DataGenerator.generateExamData(rootFolder, 1000000, 5)

  def readIds(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examIds.txt").map { line =>
    val tokens = line.split(" ")
    (tokens(0).toLong, tokens(1))
  }
    .partitionBy(new HashPartitioner(10))

  def readExamScores(): RDD[(Long, Double)] = sc.textFile(s"$rootFolder/examScores.txt").map { line =>
    val tokens = line.split(" ")
    (tokens(0).toLong, tokens(1).toDouble)
  }
    .partitionBy(new HashPartitioner(10))

  // goal is to determine the number of students who passed the exam (= at least one attempt > 9.0)

  def plainJoin() = {
    val candidates = readIds()
    val scores = readExamScores()

    // simple join
    val joined: RDD[(Long, (Double, String))] = scores.join(candidates) // (score attempt, candidate Name)
    val finalScores = joined.reduceByKey((pair1, pair2) =>
      if (pair1._1 > pair2._1) pair1 else pair2
    ).filter(_._2._1 > 9.0)

    finalScores.count()
  }

  def preAggregate() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do aggregation first
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max) // reduce to 1 fifth of the data
    maxScores.join(candidates)
      .filter(_._2._1 > 9.0)
      .count()
  }

  def preFiltering() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do aggregation first & filtering before the join
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max) // reduce to 1 fifth of the data
      .filter(_._2 > 9.0)

    val finalScores = maxScores.join(candidates)
    finalScores.count()
  }

  // repartition the RDD with the same partitioner
  def coPartitioning(): Long = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case Some(partitioner) => partitioner
      case None => new HashPartitioner(candidates.getNumPartitions)
    }

    val repartitionScore = scores.partitionBy(partitionerForScores)
    val finalScore = repartitionScore.join(candidates)
      .reduceByKey((pair1, pair2) =>
        if (pair1._1 > pair2._1) pair1 else pair2
      ).filter(_._2._1 > 9.0)

    finalScore.count()
  }

  def combined() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case Some(partitioner) => partitioner
      case None => new HashPartitioner(candidates.getNumPartitions)
    }

    val repartitionScore = scores.partitionBy(partitionerForScores)

    // do aggregation first & filtering before the join
    val maxScores: RDD[(Long, Double)] = repartitionScore.reduceByKey(Math.max) // reduce to 1 fifth of the data
      .filter(_._2 > 9.0)

    val finalScores = maxScores.join(candidates)
    finalScores.count()
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    preAggregate()
    preFiltering()
    coPartitioning()
    combined()

    Thread.sleep(100000000)
  }

}
