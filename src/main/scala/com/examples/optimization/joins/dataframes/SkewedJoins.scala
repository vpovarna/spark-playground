package com.examples.optimization.joins.dataframes

import com.examples.generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SkewedJoins {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1) // deactivate broadcast join
    .getOrCreate()

  import spark.implicits._

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1
    For each laptop configuration, we are interested in the average sale price of "similar" models.
    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
 */

  val laptops: Dataset[Laptop] = Seq.fill(40000)(DataGenerator.randomLaptop()).toDS()
  val laptopOffers: Dataset[LaptopOffer] = Seq.fill(100000)(DataGenerator.randomLaptopOffer()).toDS()

  val joined = laptops.join(laptopOffers, Seq("make", "model"))
    .filter(abs(laptopOffers.col("procSpeed") - laptops.col("procSpeed")) <= 0.1 )
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  val laptops2 = laptops.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))

  val joined2 = laptops2.join(laptopOffers, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  def main(args: Array[String]): Unit = {
    joined2.show()
    joined2.explain()

    /*
        == Physical Plan ==
        *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
        +- Exchange hashpartitioning(registration#4, 200), true, [id=#99]
           +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
              +- *(3) Project [registration#4, salePrice#20]
                 +- *(3) SortMergeJoin [make#5, model#6], [make#17, model#18], Inner, (abs((procSpeed#19 - procSpeed#7)) <= 0.1)
                    :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                    :  +- Exchange hashpartitioning(make#5, model#6, 200), true, [id=#77]
                    :     +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                    +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                       +- Exchange hashpartitioning(make#17, model#18, 200), true, [id=#78]
                          +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
     */

    Thread.sleep(1000000000)
  }


}
