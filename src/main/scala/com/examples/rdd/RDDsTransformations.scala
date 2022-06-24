package com.examples.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.internal.util.TableDef.Column

// RDD Transformations
object RDDsTransformations extends App {

  val sparkSession = SparkSession.builder()
    .appName("Rdd Transformations Examples")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  // set up the log error
  sc.setLogLevel("ERROR")

  // Read data from Scala collection
  case class Stock(symbol: String, date: String, price: Double)

  val stocksRDD: RDD[Stock] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    // filter the header
    .filter(arr => arr(0).toUpperCase() == arr(0))
    .map(arr => Stock(arr(0), arr(1), arr(2).toDouble))

  // Transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // lazy transformation

  // Min and Max
  implicit val stockOrdering: Ordering[Stock] =
    Ordering.fromLessThan((stockA: Stock, stockB: Stock) => stockA.price < stockB.price)
  val minMsft = msftRDD.min()

  // Convert from a RDD to dataframe or dataset.
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)

  // reduce operation
  val reduceRDD: Long = numbersRDD.reduce(_ + _)
  //  println(reduceRDD)

  // grouping.
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive operations

  // repartition stocks RDD
  val repartitionedStockRDD: RDD[Stock] = stocksRDD.repartition(30)
  // writing repartitioned stock file locally
  val stocksDF: DataFrame = sparkSession.createDataFrame(repartitionedStockRDD)
  //  stocksDF.write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks30")

  /**
   * Repartitioning is EXPENSIVE. Involves Shuffling.
   * Best practice: partition EARLY, then process that
   * Size of a partition: 10 -> 100MB
   */

  // Coalesce =  group RDD to smaller partitions. Does not involve shuffling because not the entire data is moved between all the partitions\
  val coalesceRDD = repartitionedStockRDD.coalesce(15)
  //    sparkSession.createDataFrame(coalesceRDD)
  //      .write
  //      .mode(SaveMode.Overwrite)
  //      .parquet("src/main/resources/data/stocks15")


  /** *
   * Exercises
   *
   * 1) Read the movies.json as a Movies RDD
   * 2) Show the distinct genres as an RDD
   * 3) Select all the movies in the Drama genre with IMDB rating > 6
   * 4) Show the average ratings of movies by genre.
   *
   */
  case class Movies(title: String, genre: String, rating: Double)

  val moviesJsonFilePath = "src/main/resources/data/movies.json"

  val moviesDF: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json(moviesJsonFilePath)

  //  moviesDF.show(100)

  // 1.

  import sparkSession.implicits._

  val moviesRDD: RDD[Movies] = moviesDF
    .select(
      col("Creative_Type").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating"))
    .where(col("rating").isNotNull and col("genre").isNotNull)
    .as[Movies]
    .rdd

  //  moviesRDD.collect().foreach(println)

  // 2.
  val genresRdd: RDD[String] = moviesRDD.map(_.genre).distinct()
  //  genresRdd.collect().foreach(println)

  // 3.
  val goodDramasRdd: RDD[Movies] = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  //  goodDramasRdd.collect().foreach(println)

  // 4.
  val moviesByGenre: RDD[(String, Double)] = moviesRDD
    .groupBy(_.genre)
    .map {
      case (genre, movies) => (genre, movies.map(_.rating).sum / movies.size)
    }

  moviesByGenre.collect().foreach(println)
}
