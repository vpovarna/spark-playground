package com.examples.streaming.dsteam

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStreamsDemo {

  val spark: SparkSession = SparkSession.builder()
    .appName("DStream")
    .config("spark.master", "local[2]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1)) // entry point to the DStream API

  // The specified duration is the batch interval. How often we're reading data.

  def readFromSocket(): Unit = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation == lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    wordsStream.print()

    // writing a stream
    // wordsStream.saveAsTextFiles("src/main/resources/data/words")

    // The above action is not started until we start the steaming context
    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile(): Unit = {
    new Thread{
      Thread.sleep(5000)
      private val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles: Int = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |MSFT,May 1 2000,25.45
          |MSFT,Jun 1 2000,32.54
          |MSFT,Jul 1 2000,28.4
          |MSFT,Aug 1 2000,28.4
          |MSFT,Sep 1 2000,24.53
          |MSFT,Oct 1 2000,28.02
          |MSFT,Nov 1 2000,23.34
          |MSFT,Dec 1 2000,17.65
          |MSFT,Jan 1 2001,24.84
          |MSFT,Feb 1 2001,24
          |MSFT,Mar 1 2001,22.25
          |MSFT,Apr 1 2001,27.56
        """.stripMargin.trim)

      writer.close()
    }.start()
  }

  def readFromFile(): Unit = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /*
      ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromFile()
  }

}
