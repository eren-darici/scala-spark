package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count}

object Wordcount {

  Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("IntroductionToDataframes")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()

    // Create SparkContext
    val sc = spark.sparkContext

    // import implicits
    import spark.implicits._

    // Read loveccraft
    val lovecraftDS = spark.read.textFile("src/main/resources/data/lovecraft.txt")
    val lovecraftDF = lovecraftDS.toDF("lovecraft")
    lovecraftDF.show(10, truncate = false)

    val words = lovecraftDS.flatMap(row => row.split(" "))
    println("Number of words: " + words.count())

    words.groupBy("value").agg(count("value")
      .as("wordCount"))
      .orderBy($"wordCount".desc)
      .show(10, truncate = false)
  }

}
