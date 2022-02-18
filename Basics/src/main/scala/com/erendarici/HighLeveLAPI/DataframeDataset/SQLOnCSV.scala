package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SQLOnCSV {
  Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("SQLOnCSV")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext


    // Load CSV file
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/data/OnlineRetail.csv")

    df.cache()

    val sqlDf = df.createOrReplaceTempView("dfTable")

    spark.sql(
      """
        SELECT Country, SUM(Quantity) as Quantity
        FROM dfTable
        GROUP BY Country
        ORDER BY Quantity DESC
      """).show()

  }

}
