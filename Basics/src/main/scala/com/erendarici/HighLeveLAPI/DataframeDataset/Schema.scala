package com.erendarici.HighLeveLAPI.DataframeDataset

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object Schema {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create a Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataframeStrings")
      .getOrCreate()

    // Create a Spark Context
    val sc = spark.sparkContext

    // Read file
    val dfFromFile = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/data/OnlineRetail.csv")

    dfFromFile.printSchema()

    val retailManuelSchema = new StructType(
      Array(
        new StructField("InvoiceNo", StringType, true),
        new StructField("StockCode", StringType, true),
        new StructField("Description", StringType, true),
        new StructField("Quantity", IntegerType, true),
        new StructField("InvoiceDate", StringType, true),
        new StructField("UnitPrice", FloatType, true),
        new StructField("CustomerID", IntegerType, true),
        new StructField("Country", StringType, true)
      )
    )

    val dfFromFile2 = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/data/OnlineRetail.csv")
      .withColumn("UnitPrice", regexp_replace(col("UnitPrice"), ",", "."))

    dfFromFile2
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .save("src/main/resources/data/OnlineRetail2")


    val dfFromManuelSchema = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .schema(retailManuelSchema)
      .load("src/main/resources/data/OnlineRetail2")

    dfFromManuelSchema.printSchema()
    dfFromManuelSchema.show()
  }

}
