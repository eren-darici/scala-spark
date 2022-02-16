package com.erendarici.LowLevelAPI.APIBasics

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    val spark = SparkSession.builder()
      .appName("CreateRDD")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create RDD from List
    println("\nCreate RDD from List")
    val rddFromList = sc.makeRDD(List(1, 2, 3, 4, 5))
    rddFromList.collect().foreach(println)

    // Create RDD from Tuple
    println("\nCreate RDD from Tuple")
    val rddFromTuple = sc.makeRDD(List((1, 2, 3), (4, 5), (6, 7, 8)))
    rddFromTuple.collect().foreach(println)

    // Create RDD from text file
    println("\nCreate RDD from text file")
    val rddFromTextFile = sc.textFile("src/main/resources/data/lorem.txt")
    rddFromTextFile.take(1).foreach(println)

  }

}
