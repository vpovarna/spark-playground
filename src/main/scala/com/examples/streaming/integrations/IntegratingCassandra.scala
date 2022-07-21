package com.examples.streaming.integrations

import com.datastax.spark.connector.cql.CassandraConnector
import com.examples.streaming.util.Car
import com.examples.streaming.util.Schema.carsSchema
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}

object IntegratingCassandra {

  val spark: SparkSession = SparkSession.builder()
    .appName("Integrating Cassandra")
    .config("spark.master", "local[2]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  // cqlsh> CREATE KEYSPACE public with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  // cqlsh> create table public.cars("Name" text PRIMARY KEY , "Horsepower" int);

  def writeStreamToCassandraInBatch(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // save the batch to cassandra in a single transaction, in static capacity
        batch
          .select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  /*
      - On every batch, on every partition `partitionId`
         - on every "epoque" = chunk of data
           - call the open method; if false, skip this chunk.
           - for each entry in this chunk, call the process method
           - call the close method at the end of the chunk or with an error it it was thrown
   */
  val keyspace = "public"
  val table = "cars"
  val connector: CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

  class CarCassandraForeachWriter extends ForeachWriter[Car] {
    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open Connection")
      true
    }

    override def process(value: Car): Unit = {
      connector.withSessionDo{ session =>
        s"""
          |insert into $keyspace.$table("Name", "Horsepower")
          |values ('${value.Name}', ${value.Horsepower.orNull})
         """.stripMargin
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing connection")
  }


  def writeStreamToCassandra(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    writeStreamToCassandra()
  }

}


