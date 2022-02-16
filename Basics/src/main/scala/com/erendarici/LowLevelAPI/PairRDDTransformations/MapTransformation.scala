package com.erendarici.LowLevelAPI.PairRDDTransformations

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
  // Create case class
  case class CancelledPrice(isCancelled: Boolean, total: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("MapTransformation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load text file
    val filePath = "src/main/resources/data/OnlineRetail.csv"
    val retailRDDWithHeader = sc.textFile(filePath)

    println("Header: " + retailRDDWithHeader.first())

    // Remove header
    val retailRDD = retailRDDWithHeader.filter(line => !line.contains("InvoiceNo"))


    // Map transformation
    val retailTotal = retailRDD.map(line => {
      val splitted = line.split(";")
      val isCancelled: Boolean = if (splitted(0).startsWith("C")) true else false
      val price: Double = splitted(5).replace(",", ".").toDouble
      val quantity: Double = splitted(3).toDouble

      val total: Double = quantity * price

      CancelledPrice(isCancelled, total)
    })

    // Cancelled orders
    println("Total price of cancelled orders:")
    retailTotal.map(item => (item.isCancelled, item.total))
      .reduceByKey((x, y) => x + y)
      .filter(item => item._1)
      .map(x => x._2)
      .take(1)
      .foreach(println)
  }
}
