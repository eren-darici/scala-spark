package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}


object DateTimeOps {
  Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)


  def main(args: Array[String]): Unit = {

    // Create a Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataframeStrings")
      .getOrCreate()


    // Create a Spark Context
    val sc = spark.sparkContext

    // Read file
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/data/OnlineRetail.csv")
      .select("InvoiceDate")
      .distinct()
    // DD-MM-YYYY

    println("Dataframe:")
    df.show(25)

    val currentFormat = "dd.MM.yyyy HH:mm"
    val formatTR = "dd/MM/yyyy HH:mm:ss"
    val formatEN= "MM/dd/yyyy HH:mm:ss"

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val df2 = df.withColumn("InvoiceDate", F.trim(F.col("InvoiceDate")))
      .withColumn("NormalDate",F.to_date(F.col("InvoiceDate"), currentFormat))
      .withColumn("StandardTS", F.to_timestamp(F.col("InvoiceDate"), currentFormat))
      .withColumn("UnixTS", F.unix_timestamp(F.col("StandardTS"), currentFormat))
      .withColumn("TRDate", F.date_format(F.col("StandardTS"), formatTR))
      .withColumn("ENDate", F.date_format(F.col("StandardTS"), formatEN))

    df2.show(5)

  }

}
