package com.erendarici.LowLevelAPI.APIBasics

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object MapFlatMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val conf = new SparkConf().setAppName("MapFlatMap").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create a RDD
    val filePath: String = "src/main/resources/data/simple_data.csv"
    val retailRDD = sc.textFile(filePath)
      .filter(!_.contains("sirano"))

    println("Original RDD:")
    retailRDD.take(5).foreach(println)

    // Map (for every line in textFile)
    println("\nMapped RDD:")
    retailRDD.map(x => x.toUpperCase()).take(5).foreach(println)

    // FlatMap (for every element in textFile)
    println("\nFlatMapped RDD:")
    retailRDD.flatMap(x => x.split(",")).take(6).foreach(println)
  }

}
