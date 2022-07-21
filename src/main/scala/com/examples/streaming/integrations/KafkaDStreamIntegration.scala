package com.examples.streaming.integrations

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object KafkaDStreamIntegration {

  val spark: SparkSession = SparkSession.builder()
    .appName("Kafka DStream Integration")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // sending data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receive data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic: String = "rockthejvm"

  def readFromKafka(): Unit = {
    val topics = Array(kafkaTopic)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
          Distributes the partitions evenly across the Spark cluster
          Alternatives:
            - PreferBrokers if the brokers are executors are in the same cluster
            - PreferFixed
       */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
          Alternatives:
            - SubscribePattern allows subscribing to topics matching a pattern
            - Assign - advanced; allows specifying offsets and partitions per topic
       */
    )

    val processStream = kafkaDStream.map(record => (record.key(), record.value()))
    processStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka(): Unit = {
    val inputData = ssc.socketTextStream("localhost", 12345)
    val processedData = inputData.map(_.toUpperCase)

    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        //the code is run by single executors
        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into the Kafka topics
        // available on this executors
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach{ value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          producer.send(message)
        }

        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

}
