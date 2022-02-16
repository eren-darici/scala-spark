package com.erendarici.LowLevelAPI.PairRDDTransformations

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Joins {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Creating SparkContext
    val conf = new SparkConf().setAppName("Joins").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Read orders
    val orderItemsRDD = sc.textFile("src/main/resources/data/retail_db/order_items.csv")
      .filter(!_.contains("orderItemName")) // Skip header

    println("First look at order items: ")
    orderItemsRDD.take(5).foreach(println)

    // Read products
    val productsRDD = sc.textFile("src/main/resources/data/retail_db/products.csv")
      .filter(!_.contains("productName")) // Skip header

    println("\nFirst look at products: ")
    productsRDD.take(5).foreach(println)

    // Orders pair RDD
    val orderItemsPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)
    println("\nFirst look at order items pair RDD: ")
    orderItemsPairRDD.take(5).foreach(println)

    // Products pair RDD
    val productsPairRDD = productsRDD.map(makeProductsPairRDD)
    println("\nFirst look at products pair RDD: ")
    productsPairRDD.take(5).foreach(println)

    // JOIN
    val orderItemProductJoinedRDD = orderItemsPairRDD.join(productsPairRDD)
    println("\nFirst look at joined RDD: ")
    orderItemProductJoinedRDD.take(5).foreach(println)
  }



}
