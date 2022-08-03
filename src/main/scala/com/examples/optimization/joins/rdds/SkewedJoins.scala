package com.examples.optimization.joins.rdds

import com.examples.generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SkewedJoins {

  val spark: SparkSession = SparkSession.builder()
    .appName("RDD Skewed Joins")
    .master("local[2]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("WARN")

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1
    For each laptop configuration, we are interested in the average sale price of "similar" models.
    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
 */

  val laptops: RDD[Laptop] = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
  val laptopOffers: RDD[LaptopOffer] = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

  def plainJoin(): Long = {

    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }
    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make,value), ((req, cpu), (cpu, salePrice))]
      .filter {
        case ((make, model), ((req, laptopCPU), (offerCPU, salePrice))) => Math.abs(laptopCPU - offerCPU) <= 0.1
      }
      .map {
        case ((make, model), ((req, laptopCPU), (offerCPU, salePrice))) => (req, salePrice)
      }
      .aggregateByKey((0.0, 0))(
        {
          case ((totalSalePrice, numPrices), salePrice) => (totalSalePrice + salePrice, numPrices + 1) // combine state with records
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine two different states into one state if you fetch them from a single partition
        }
      ) // RDD[(String, (Double, Int)]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices // in order to return the avg.
      }

    result.count()
  }

  def noSkewJoin() = {
    val preparedLaptops = laptops.flatMap(laptop => Seq(
      laptop,
      laptop.copy(procSpeed = laptop.procSpeed - 0.1),
      laptop.copy(procSpeed = laptop.procSpeed + 0.1)
    )).map {
      case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), registration)
    }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedOffers)
      .map(_._2)
      .aggregateByKey((0.0, 0))(
        {
          case ((totalSalePrice, numPrices), salePrice) => (totalSalePrice + salePrice, numPrices + 1) // combine state with records
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine two different states into one state if you fetch them from a single partition
        }
      ) // RDD[(String, (Double, Int)]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices // in order to return the avg.
      }

    result.count()
  }

  def main(args: Array[String]): Unit = {
//    plainJoin()
    noSkewJoin()
    Thread.sleep(1000000)
  }

}
