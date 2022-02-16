package com.erendarici.LowLevelAPI.APIBasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingRDD")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create RDD from text file
    val sonnetsRDD = sc.textFile("src/main/resources/data/sonnets.txt")
    println(sonnetsRDD.count())

    // Split each line into words
    val wordsRDD = sonnetsRDD.flatMap(line => line.split(" "))

    // Count the number of times each word occurs
    val wordCountsRDD = wordsRDD.map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
    println(wordCountsRDD.count())

    // Sort the counts in descending order
    val sortedWordCountsRDD = wordCountsRDD.sortBy(wordCount => wordCount._2, ascending = false)

    // Take top 10 and print
    sortedWordCountsRDD.take(10).foreach(println)

  }
}
