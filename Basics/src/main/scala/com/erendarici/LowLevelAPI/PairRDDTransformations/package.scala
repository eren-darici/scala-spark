package com.erendarici.LowLevelAPI

package object PairRDDTransformations {
  def makeOrderItemsPairRDD(line: String) = {
    val splitted = line.split(",")

    val orderItemName = splitted(0)
    val orderItemOrderId = splitted(1)
    val orderItemProductId = splitted(2)
    val orderItemQuantity = splitted(3)
    val orderItemSubTotal = splitted(4)
    val orderItemProductPrice = splitted(5)

    (orderItemProductId, (orderItemName, orderItemOrderId, orderItemProductId, orderItemQuantity, orderItemSubTotal, orderItemProductPrice))
  }

  def makeProductsPairRDD(line: String) = {
    val splitted = line.split(",")

    val productId = splitted(0)
    val productCategoryId = splitted(1)
    val productName = splitted(2)
    val productDescription = splitted(3)
    val productPrice = splitted(4)
    val productImage = splitted(5)

    (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
  }

}
