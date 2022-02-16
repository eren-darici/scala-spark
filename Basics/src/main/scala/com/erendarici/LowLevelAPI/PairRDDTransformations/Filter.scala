package com.erendarici.LowLevelAPI.PairRDDTransformations

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create a SparkContext
    val conf = new SparkConf().setAppName("filter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filePath = "src/main/resources/data/OnlineRetail.csv"

    val retailRDDWithHeader = sc.textFile(filePath)
    retailRDDWithHeader.take(1).foreach(println)

    // Number of lines with header
    println("Number of lines with header: " + retailRDDWithHeader.count())

    // Remove header
    val retailRDD = retailRDDWithHeader.mapPartitionsWithIndex(
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    )

    // Number of lines without header
    println("Number of lines without header: " + retailRDD.count())

    // Filter quantity < 30
    println("\n===== Lines with quantity > 30 =====\n")
    retailRDD.filter(line => line.split(";")(3).toInt > 30)
      .take(5)
      .foreach(println)

    // Filter COFFEE products with UnitPrice > 20.0
    println("\n===== Lines with COFFEE products and UnitPrice > 20.00 =====\n")
    retailRDD.filter(line => coffePrice20(line))
      .take(5)
      .foreach(println)
  }

  def coffePrice20(line: String): Boolean = {
    var result = true
    var Description: String = line.split(";")(2)
    var UnitPrice: Double = line.split(";")(5)
      .trim
      .replace(",", ".")
      .toDouble

    result = Description.contains("COFFEE") && UnitPrice > 20.0

    result
  }


}
