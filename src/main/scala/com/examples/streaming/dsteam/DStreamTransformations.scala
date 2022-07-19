package com.examples.streaming.dsteam

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamTransformations {

  val spark: SparkSession = SparkSession.builder()
    .appName("DStream Transformation")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("ERROR")
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

  def readPeople() = ssc.socketTextStream("localhost", 9999)
    .map(line => {
      val tokens = line.split(":")
      Person(
        tokens(0).toInt,
        tokens(1),
        tokens(2),
        tokens(3),
        tokens(4),
        Date.valueOf(tokens(5)),
        tokens(6),
        tokens(7).toInt,
      )
    })

  def peopleAges(): DStream[(String, Int)] = readPeople().map(person => {
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  })

  def peopleSmallName(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople(): DStream[Person] = readPeople().filter(person => person.salary > 80000)

  def countPeople(): DStream[Long] = readPeople().count() // returns the number of people in every batch

  def countNames(): DStream[(String, Long)] = readPeople().map(_.firstName).countByValue() // operates per BATCH

  def countNamesReduce(): DStream[(String, Int)] = readPeople()
    .map(_.firstName)
    .map(name => (name, 1))
    .reduceByKey(_ + _)

  // countByValue is implemented as reduceByKey based on the tuple.
  import spark.implicits._

  def saveToJson() = readPeople()
    .foreachRDD{ rdd =>
      val ds = spark.createDataset(rdd) // creates a micro dataset per batch
      val p: String = "src/main/resources/data/people"
      val f = new File(p)
      val nFiles = f.listFiles().length
      val path = s"$p/people$nFiles.json"
      ds.write.json(path)
    }

  def main(args: Array[String]): Unit = {
    val stream = peopleSmallName()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
