package com.erendarici.LowLevelAPI.APIBasics.challenge

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object BasicsChallenge {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    val spark = new SparkConf()
      .setAppName("BasicsChallenge")
      .setMaster("local[2]")
      .set("spark.driver.memory", "2g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(spark)

    val filePath = "src/main/resources/data/lovecraft.txt"
    fileReadChallenge(filePath, sc)
    println("===============================")
    rddChallenge(sc)
  }

  def fileReadChallenge(filePath: String, sc: SparkContext): Unit = {
    val file = sc.textFile(filePath)

    // 1. How many lines are in the file?
    val lines = file.count()
    println("Number of lines: " + lines)

    // 2. How many words are in the file?
    val wordsRDD = file.flatMap(line => line.split(" "))
    val words = wordsRDD.count()
    println("Number of words: " + words)
  }

  def rddChallenge(sc: SparkContext): Unit = {
    // Get intersection of two RDDs
    val rdd = sc.parallelize(List(3, 7, 13, 15, 22, 36, 7, 11, 3, 25))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    println("Intersection:")
    rdd.intersection(rdd2).foreach(println)

    // Get distinct numbers from first RDD
    println("Distinct numbers:")
    rdd.distinct().foreach(println)

    // Get occurences of each number in first RDD
    println("Occurences:")
    rdd.map(x => (x, 1)).reduceByKey((x, y) => x + y).foreach(println)


  }

}
