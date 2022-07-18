package com.examples.essentials.practical

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxaApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("TaxiApp application")
    .getOrCreate()

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  //  println(taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.printSchema()

  /**
   * Questions:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */

  // 1. Which zones have the most pickups/dropoffs overall?
  val pickupsByTaxiZoneDF = taxiDF
    .groupBy(col("PULocationID"))
    .agg(count("*").as("TotalTrips"))
    .join(taxiZonesDF, taxiDF.col("PULocationID") === taxiZonesDF.col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("TotalTrips").desc_nulls_last)

  //  pickupsByTaxiZoneDF.show()

  // 1b - group by borough
  val pickupByBorough = pickupsByTaxiZoneDF
    .groupBy(col("Borough"))
    .agg(sum(col("TotalTrips")).as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

  //  pickupByBorough.show()

  // 2. What are the peak hours for taxi?
  val pickupsByHourDF = taxiDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

  // pickupsByHourDF.show()

  // 3.
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30

  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )

  // tripDistanceStatsDF.show()

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  //  tripsByLengthDF.show()

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

  //  pickupsByHourByLengthDF.show(48)

  //5
  def pickupDropOffPopularity(predicate: Column) =
    tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("TotalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "serviceZone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("TotalTrips").desc_nulls_last)

  val pickupDropPopularityShortDF = pickupDropOffPopularity(not(col("isLong")))
  val pickupDropPopularityLongDF = pickupDropOffPopularity(col("isLong"))

  //  pickupDropPopularityShortDF.show(truncate = false)
  //  pickupDropPopularityLongDF.show(truncate = false)

  // 6
  val rateCodeDistributionDF = taxiDF.groupBy(col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //  rateCodeDistributionDF.show()

  // 7
  val rateCodeEvolutionDF = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("pickup_day").desc_nulls_last)

  // 8
  // Group taxiDF dataframe by pickup location ID on the 5 minutes window.
  val groupAttemptsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("PULocationID"), col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  groupAttemptsDF.show()

  import spark.implicits._

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimatingEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  groupingEstimatingEconomicImpactDF.show(100)

  val totalProfitDF = groupingEstimatingEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  totalProfitDF.show()
}
