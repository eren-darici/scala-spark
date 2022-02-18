package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataToDisk {
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

    val cleanDF = dirtyDF
      .withColumn("isim", trim(initcap(col("isim"))))
      .withColumn("cinsiyet", when(col("cinsiyet").isNull, "O").otherwise(col("cinsiyet")))
      .withColumn("sehir", when(col("sehir").isNull, "Bilinmiyor").otherwise(col("sehir")))
      .withColumn("sehir", trim(upper(col("sehir"))))

    cleanDF.show()

    cleanDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("sep", ",")
      .format("csv")
      .save("src/main/resources/data/simple_data_clean")
  }
}
