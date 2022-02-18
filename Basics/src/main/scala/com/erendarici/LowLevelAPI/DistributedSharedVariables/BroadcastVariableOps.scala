package com.erendarici.LowLevelAPI.DistributedSharedVariables

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

import scala.io.Source

object BroadcastVariableOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create a SparkContext
    val sc = new SparkContext("local[*]", "BroadcastVariableOps")
    println(loadProducts())

    // create a broadcast variable from a collection
    val products = sc.broadcast(loadProducts())

    // Read order_items.csv
    val orderItems = sc.textFile("src/main/resources/data/retail_db/order_items.csv")
      .filter(!_.contains("orderItemName"))

    // creating PairRDD with function
    val orderItemPairRDD = orderItems.map(makeOrderItemsPairRdd)


    // Reduce by key
    val sortedOrders = orderItemPairRDD.reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))

    val sortedOrdersWithProductName = sortedOrders.map(x => (products.value(x._1), x._2))
    sortedOrdersWithProductName.take(10).foreach(println)
  }


}
