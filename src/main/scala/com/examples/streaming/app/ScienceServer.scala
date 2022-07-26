package com.examples.streaming.app

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import java.util.Properties
import scala.io.Source

object ScienceServer {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val kafkaTopic: String = "science"
  val kafkaBootStrapServer = "localhost:9092"

  def getProducer(): KafkaProducer[Long, String] = {
    val properties: Properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServer)
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    new KafkaProducer[Long, String](properties)
  }

  def getRoute(producer: KafkaProducer[Long, String]): Route = {
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString(""))
        )
      }
    } ~
      path("api" / "report") {
        (parameter("sessionId".as[String]) & parameter("time".as[Long])) {
          (sessionId: String, time: Long) =>
            println(s"I've found session ID: ${sessionId} and time = $time")
            val record = new ProducerRecord[Long, String](kafkaTopic, 0, s"$sessionId,$time")
            producer.send(record)
            producer.flush()
            complete(StatusCodes.OK)
        }
      }
  }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // spinning up the server
    val kafkaProducer = getProducer()
    val bindingFuture = Http().bindAndHandle(getRoute(kafkaProducer), "localhost", 9988)

    // cleanup
    bindingFuture.foreach{ biding =>
      biding.whenTerminated.onComplete(_ => kafkaProducer.close())
    }
  }
}
