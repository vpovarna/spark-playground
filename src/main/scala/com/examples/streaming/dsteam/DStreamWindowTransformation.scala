package com.examples.streaming.dsteam

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DStreamWindowTransformation {

  val spark: SparkSession = SparkSession.builder()
    .appName("Window Transformations")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

  def readLine(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  /*
        A window keeps all the values emitted between now and x tome back
        Window interval must be a multiple of the batch interval
        Window interval updated with every batch
   */
  def linesByWindow(): DStream[String] = readLine().window(Seconds(10)) // window = now - 10 seconds back

  /*
        Window duration and sliding durations
   */
  def linesBySlidingWindow(): DStream[String] = readLine().window(Seconds(10), Seconds(5))

  // count the number of clicks
  def countLinesByWindow(): DStream[Long] = readLine().window(Minutes(60), Seconds(30)).count()

  // in spark this combination is equivalent with countByWindow
  def countLinesByWindow2(): DStream[Long] = readLine().countByWindow(Minutes(60), Seconds(30))

  // aggregate data in different way over window
  def sumAllTextByWindow(): DStream[Int] = readLine().map(_.length).window(Seconds(10), Seconds(5)).reduce(_ + _)

  // in spark this combination is equivalent with
  def sumAllTextByWindow2(): DStream[Int] = readLine().map(_.length).reduceByWindow(_ + _, Seconds(10), Seconds(5))

  // Tumbling windows doesn't have slide interval
  def linesByTumblingWindow(): DStream[String] = readLine().window(Seconds(10), Seconds(10)) // batch of batches

  // common words that occurs every hour and update every 30 sec
  def computeWordOccurrencesByWindow(): DStream[(String, Int)] = {
    ssc.checkpoint("checkpoints") // for reduceByKeyAndWindow you need a checkpoint directory set.

    readLine()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // "inverse" function -> counting out the values that are falling down from the window
        Seconds(60), // Window duration
        Seconds(30) // Sliding duration
      )
  }

  /**
   * Exercise:
   * word longer than 10 chars => 2$
   * every other word 0$
   *
   * Input text into the terminal => money made over the past 30 seconds, updated every 10 seconds
   *      - use window
   *      - use count by window
   */

  val moneyPerExpensiveWord = 2
  val characterLength = 10

  def showMeTheMoney(): DStream[Int] = {
    readLine().flatMap(_.split(" "))
      .filter(_.length >= characterLength)
      .map(_ => moneyPerExpensiveWord)
      .reduce(_ + _) // optimization
      .window(Seconds(30), Seconds(10))
      .reduce(_ + _)
  }

  def showMeTheMoney2(): DStream[Long] = {
    readLine().flatMap(_.split(" "))
      .filter(_.length >= characterLength)
      .countByWindow(Seconds(30), Seconds(10))
      .map(_ * moneyPerExpensiveWord)
  }

  def showMeTheMoney3(): DStream[Int] = {
    readLine().flatMap(_.split(" "))
      .filter(_.length >= characterLength)
      .map(_ => moneyPerExpensiveWord)
      .reduceByWindow(_ + _, Seconds(30), Seconds(10))
  }

  def showMeTheMoney4(): DStream[(String, Int)] = {
    ssc.checkpoint("checkpoints")

    readLine().flatMap(_.split(" "))
      .filter(_.length >= characterLength)
      .map { word => {
        if (word.length >= characterLength) ("expensive", 2)
        else ("cheap", 2)
      }
      }
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(10))
  }


  def main(args: Array[String]): Unit = {
    computeWordOccurrencesByWindow().print()

    ssc.start()
    ssc.awaitTermination()
  }

}
