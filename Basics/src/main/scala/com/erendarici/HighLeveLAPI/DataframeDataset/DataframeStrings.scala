package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeStrings {
  Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

  def main(args: Array[String]): Unit = {

    // Create a Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataframeStrings")
      .getOrCreate()

    // Create a Spark Context
    val sc = spark.sparkContext

    // Read dirty data
    val dirtyDF = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("src/main/resources/data/simple_data_dirty.csv")

    dirtyDF.show()


    // trim
    val dirtyDFTrimmed = dirtyDF.withColumn("sehir", trim(col("sehir")))

    // Concat
    dirtyDFTrimmed.select("meslek", "sehir")
      .withColumn("meslek_sehir", concat(col("meslek"), lit(" - "), col("sehir")))
      .show(truncate = false)
  }

}
