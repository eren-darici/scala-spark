package com.erendarici.LowLevelAPI.PairRDDTransformations

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object PairRDDOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    // Create SparkContext
    val sc = new SparkContext("local[*]", "PairRDDOps")

    // Create RDD
    val peopleRDD = sc.textFile("src/main/resources/data/simple_data.csv").filter(line => !line.contains("aylik_gelir"))

    // Map function
    val jobIncomePairRDD = peopleRDD.map(jobIncomePair)

    println("Job-Income Pair RDD:")
    jobIncomePairRDD.take(5).foreach(println)


    //  mapValues
    println("\nIncome by job:")
    val incomeByJobRDD = jobIncomePairRDD.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    incomeByJobRDD.take(5).foreach(println)


    // Average income by job
    println("\nAverage income by job:")
    val averageIncomeByJobRDD = incomeByJobRDD.mapValues(x => x._1 / x._2)
    averageIncomeByJobRDD.take(5).foreach(println)


  }



  def jobIncomePair(line: String): (String, Double) = {
    val splitted = line.split(",")

    val job = splitted(3)
    val income = splitted(5).toDouble

    (job, income)
  }

}
