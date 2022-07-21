package com.examples.streaming.strucured

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.Socket
import scala.concurrent.{Future, Promise}
import scala.io.Source

class CustomSocketReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global
  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture: Future[Socket] = socketPromise.future

  // called async
  override def onStart(): Unit = {
    val socket = new Socket(host, port)

    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store makes the string available to Spark
    }

    // called async
    socketPromise.success(socket)
  }

  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}

object CustomReceiverApp {
  val spark: SparkSession = SparkSession.builder()
    .appName("Custom receiver app")
    .master("local[*]")
    .getOrCreate()
  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {
    val dataStream: DStream[String] = ssc.receiverStream(new CustomSocketReceiver("localhost", 12345))
    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
