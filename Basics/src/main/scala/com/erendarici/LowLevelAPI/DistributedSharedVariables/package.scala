package com.erendarici.LowLevelAPI

import scala.io.Source

package object DistributedSharedVariables {

  // Read file with Scala IO
  def loadProducts(): Map[Int, String] = {
    val source = Source.fromFile("src/main/resources/data/retail_db/products.csv")

    val lines = source.getLines()
      .filter(x => !x.contains("productCategoryId"))

    var productIdAndName: Map[Int, String] = Map()

    for (line <- lines) {
      val splitted = line.split(",")
      val productId = splitted(0).toInt
      val productName = splitted(2)

      productIdAndName += (productId -> productName)
    }

    productIdAndName
  }

  // Make order items pair RDD
  def makeOrderItemsPairRdd(line: String): (Int, Float) = {
    val splitted = line.split(",")
    val orderItemName = splitted(0)
    val orderItemOrderId = splitted(1)
    val orderItemProductId = splitted(2).toInt
    val orderItemQuantity = splitted(3)
    val orderItemSubTotal = splitted(4).toFloat
    val orderItemProductPrice = splitted(5)

    (orderItemProductId, orderItemSubTotal)
  }

}
