package com.revature.q5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Runner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("<App Name Here>")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

      if (args(0).toInt == 1) {
        createBucket.start(spark)
      } 
      else if (args(0).toInt == 2) {
        countTechJobs.start(spark)
      }
      else if (args(0).toInt == 3) {
        countKeywords.start(spark)
      }
      else
        println("Valid input not provided.\n" +
          "You can use one of the following numbers as valid input for this program:\n" +
          "1 - Creates a list of csv files in the currently coded bucket.\n" +
          "2 - Counts the total number of jobs associated with certain keywords in an S3 bucket\n" +
          "3 - Counts the total number of jobs grouped by keyword in an S3 bucket\n")
        System.exit(0)
  }
}
