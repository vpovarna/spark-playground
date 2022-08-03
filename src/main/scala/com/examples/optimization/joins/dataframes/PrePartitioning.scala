package com.examples.optimization.joins.dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.lang

object PrePartitioning {

  val spark: SparkSession = SparkSession.builder()
    .appName("Pre Partitioning")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  // add new n columns to the df
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }

  // Don't ouch this
  val initialTable: Dataset[lang.Long] = spark.range(1, 10000000).repartition(10) // RoundRobinPartition(10)
  val narrowTable: Dataset[lang.Long] = spark.range(1, 5000000).repartition(7) // RoundRobinPartition(7)

  // scenario1
  val wideTable: DataFrame = addColumns(initialTable, 30)
  val join1: DataFrame = wideTable.join(narrowTable, "id")
  join1.explain()
//  println(join1.count())

  //scenario2
  val allNarrow: Dataset[lang.Long] = narrowTable.repartition(col("id"))
  val allInitial: Dataset[lang.Long] = narrowTable.repartition(col("id"))
  val join2: DataFrame = allInitial.join(allNarrow, "id")
  val result2: DataFrame = addColumns(join2, 30)
  join2.explain()
//  println(result2.count())

  // scenario3
  // the problem is that we added the 30 columns and then we repartitioned.

  val enhancedColumnFirst: DataFrame = addColumns(initialTable, 30)
  val repartitionedNarrowed: Dataset[lang.Long] = narrowTable.repartition(col("id"))

  val repartitionEnhanced: Dataset[Row] = enhancedColumnFirst.repartition(col("id"))
  val join3: DataFrame = repartitionEnhanced.join(repartitionEnhanced, "id")
  join3.explain()

  // partitioning late is at best what spark will do.

  def main(args: Array[String]): Unit = {

  }

}
