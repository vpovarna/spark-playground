package com.examples.essentials.rdd

import com.examples.utils.using
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source
import scala.language.reflectiveCalls

// Creating RDDs
object RDDs extends App {

  val sparkSession = SparkSession.builder()
    .appName("Rdd Examples")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  // set up the log error
  sc.setLogLevel("ERROR")

  // Read data from Scala collection
  case class Stock(symbol: String, date: String, price: Double)

  def readStocks(filename: String): List[Stock] =
    using(Source.fromFile(filename)) { source =>
      source.getLines()
        .drop(1)
        .map(line => line.split(","))
        .map(arr => Stock(arr(0), arr(1), arr(2).toDouble))
        .toList
    }

  // method 1: Using parallelize
  val stocksRdd: RDD[Stock] = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  stocksRdd.collect().foreach(println)

  // method 2: Using textFile method.
  val stockRdd2: RDD[Stock] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    // filter the header
    .filter(arr => arr(0).toUpperCase() == arr(0))
    .map(arr => Stock(arr(0), arr(1), arr(2).toDouble))

  // 3. Read from a dataframe
  val stockDF = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  // Convert as dataset

  import sparkSession.implicits._

  val stocksDS: Dataset[Stock] = stockDF.as[Stock]
  val stockRdd3: RDD[Stock] = stocksDS.rdd

  // You can convert from DF to RDD by applying the same method, but you'll loose the scala class type, you'll obtain a dataframe of rows

  // Convert from a RDD to dataframe or dataset.
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)
  // RDD -> DF. Column names can be pass into the toDF method
  val numbersDF: DataFrame = numbersRDD.toDF("number")

  // RDD -> Ds
  val numbersDS: Dataset[Int] = sparkSession.createDataset(numbersRDD) // you are not loosing the type information

  /**
   * RDD vs Datasets (Dataframes are Datasets[ROW])
   * In common:
   * -> collection API: map, flatMap, filter, take, reduce, etc
   * -> union, Count, districts
   * -> groupBy, sortBy
   *
   * RDDs over Datasets:
   * -> partition control: repartition, coalesce, partitioner, zipPartitions, mapPartitions
   * -> operation control: checkpoint, isCheckpointed, cache
   * -> storage control: cache, getStorageLevel, persist
   *
   * Datasets over RDDs:
   * -> select and join!
   * -> Spark is planning/optimizing before running the code.
   */


}
