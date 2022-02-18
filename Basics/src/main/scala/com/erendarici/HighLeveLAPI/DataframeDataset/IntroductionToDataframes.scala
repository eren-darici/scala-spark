package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object IntroductionToDataframes {
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

    // Import implicits
    import spark.implicits._

    // Create dataframe from list (rdd)
    val dfFromList = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("number")
    dfFromList.printSchema()

    // Create another dataframe from range
    val dfFromList2 = sc.range(10, 100, 5).toDF("number")
    dfFromList2.printSchema()

    // Create dataframe from file
    val filePath: String = "src/main/resources/data/OnlineRetail.csv"

    val dfFromFile = spark.read.format("csv")
      .options(Map(
        "header" -> "true",
        "inferSchema" -> "true",
        "delimiter" -> ";"
      ))
      .load(filePath)

    dfFromFile.show(10)
    dfFromFile.printSchema()

    // Number of observations
    println("Number of observations: " + dfFromFile.count())

    // Select columns
    dfFromFile.select("CustomerID", "Country").show(10)

    // Sort by column
    dfFromFile.sort("Quantity").show(10)

    // Select stock code sorted by quantity
    dfFromFile.select("StockCode").sort("Quantity").show(10)

    // Dynamic configuration and change shuffle partition number
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    // explain(): How to do that job?
    dfFromFile.sort($"Quantity").explain()

  }

}
