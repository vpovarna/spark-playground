package com.examples.optimization.joins.rdds

import akka.stream.TLSProtocol.SessionTruncated
import com.examples.optimization.joins.rdds.SimpleRDDJoins.{rootFolder, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CoGrouping {

  val spark: SparkSession = SparkSession.builder()
    .appName("CoGrouping RDDs")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  /*
      Take all the student attempts
        - if the student passed (at least one attempt > 9.0), send them an email "PASSED"
        - else send them an email with "FAILED"
   */

  val rootFolder = "src/main/resources/data/generated/examData"

  def readIds(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examIds.txt").map { line =>
    val tokens = line.split(" ")
    (tokens(0).toLong, tokens(1))
  }

  def readExamScores(): RDD[(Long, Double)] = sc.textFile(s"$rootFolder/examScores.txt").map { line =>
    val tokens = line.split(" ")
    (tokens(0).toLong, tokens(1).toDouble)
  }

  def readExampleEmail(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examEmails.txt").map { line =>
    val tokens = line.split(" ")
    (tokens(0).toLong, tokens(1))
  }

  // Naive solution
  def plainJoin(): Long = {
    val scores = readExamScores().reduceByKey(Math.max) // get the maximum of the 5 attempts
    val candidates = readIds()
    val emails = readExampleEmail()

    val results: Long = candidates
      .join(scores)
      .join(emails)
      .mapValues {
        case ((_, maxAttempt), email) =>
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }.count()

    results
  }

  def coGroupedJoin(): Long = {
    val scores = readExamScores().reduceByKey(Math.max) // get the maximum of the 5 attempts
    val candidates = readIds()
    val emails = readExampleEmail()

    val result = candidates.cogroup(scores, emails) // we'll co partitions the 3 RDDs
      .mapValues{
        case (nameIterable, maxAttemptIterable, emailIterable) =>
          val name = nameIterable.headOption
          val maxScore = maxAttemptIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 0.9) "PASSED" else "FAILED")
      }

    result.count()
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    coGroupedJoin()
    Thread.sleep(100000000)
  }

}
