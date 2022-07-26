package com.examples.streaming.advanced

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{Dataset, SparkSession}

object StatefulComputation {
  val spark: SparkSession = SparkSession.builder()
    .appName("Stateful Computation")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERRORte")

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)

  case class SocialPostBulk(postType: String, count: Int, totalStorageUsage: Int)

  case class AveragePostStorage(postType: String, averageStorage: Double)

  import spark.implicits._

  def readSocialUpdates(): Dataset[SocialPostRecord] =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
      }

  def updateAverageStorage(postType: String, group: Iterator[SocialPostRecord], state: GroupState[SocialPostBulk]): AveragePostStorage = {
    /**
     *  - extract the state to start with
     *  - for all the items in the group, aggregate data:
     *      - summing up the total count of the post
     *      - summing up the total storage
     *  - update the state with the new aggregated data
     *  - return a single value of type AveragePostStorage: TotalStorageUsed / totalCount
     */

    // extracted the state to start with
    val previousBulk =
    if (state.exists) state.get
    else SocialPostBulk(postType = postType, count = 0, totalStorageUsage = 0)

    // iterate through the group
    val totalAggregatedData: (Int, Int) = group.foldLeft((0, 0)) { (currentData, record) =>
      val (currentCount, currentStorage) = currentData
      (currentCount + record.count, currentStorage + record.storageUsed)
    }
    val (totalCount, totalStorage) = totalAggregatedData

    //update the state with the aggregated data
    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totalCount, previousBulk.totalStorageUsage + totalStorage)
    state.update(newPostBulk)

    // return a single output value
    AveragePostStorage(postType, newPostBulk.totalStorageUsage * 1.0 / newPostBulk.count)
  }

  def getAveragePostStorage(): Unit = {
    val socialStream = readSocialUpdates()

    val regularSQLAverageByPostType = socialStream
      .groupByKey(_.postType)
      .agg(sum(col("count")).as("totalCount").as[Int], sum(col("storageUsed")).as("totalStorage").as[Int])
      .selectExpr("key as postType", "totalStorage/totalCount as avgStorage")

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)


    averageByPostType.writeStream
      .format("console")
      .outputMode("update") // append not supported on mapGroupsWithState
      .foreachBatch{ (batch: Dataset[AveragePostStorage], batchId: Long) => batch.show() }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAveragePostStorage()
  }
}
