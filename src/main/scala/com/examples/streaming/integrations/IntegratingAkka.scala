package com.examples.streaming.integrations

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.examples.streaming.util.Car
import com.examples.streaming.util.Schema.carsSchema
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingAkka {
  val spark: SparkSession = SparkSession.builder()
    .appName("Integrating Akka")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  def writeCarsToAkka(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.foreachPartition { cars: Iterator[Car] =>
          // this code is run by a single executor

          val system: ActorSystem = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          // send all the data
          cars.foreach(car => entryPoint ! car)
        }
      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    writeCarsToAkka()
  }
}

object ReceiverSystem {
  implicit val actorSystem: ActorSystem = ActorSystem("ReceiverSystem", ConfigFactory.load("akkaconfig/remoteActors").getConfig("remoteSystem"))
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive: Receive = {
      case m => log.info(m.toString)
    }
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {
      case m =>
        log.info(s"Received $m")
        destination ! m
    }
  }

  object EntryPoint {
    def pops(destination: ActorRef): Props = Props(new EntryPoint(destination))
  }

  def main(args: Array[String]): Unit = {
    // using actor system
    //    val destination = actorSystem.actorOf(Props[Destination], "destination")
    //    val entryPoint = actorSystem.actorOf(EntryPoint.pops(destination), "entrypoint")

    // using akka streams
    val source = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Car](println)
    val runnableGraph = source.to(sink)
    val actorRef: ActorRef = runnableGraph.run()

    val entryPoint = actorSystem.actorOf(EntryPoint.pops(actorRef), "entrypoint")

  }
}