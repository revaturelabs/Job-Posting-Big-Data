package com.revature.q5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This runner file searches all csv files in a given directory,
  * and keeps a count of each url that has a specific keyword and job in it
  * as a total count of all jobs.
  * 
  * Results are written out to the console.
  */
object countTechJobs {
  def start(spark: SparkSession) {

  import spark.implicits._

  val columnarIndexDF = spark
    .read
    .format("csv")
    .option("header", "true")
    // Change this line to point at a different bucket/folder in s3
    .csv("s3://path/to/your/folder/*.csv")

  columnarIndexDF
    .select(lower($"url_path").as("url"), $"crawl")
    // Change the filter to get different data, either by month or by year
    // NOTE: Common Crawl's Columnar Index data was listed by week 
    //  Example: 2020-05 is the fifth week of 2020, and the crawl for January 2020
    .filter($"crawl" like "%YYYY-WW%")
    .filter(($"url" like "%job%") 
      and (($"url" like "%software%")
        or ($"url" like "%frontend%")
        or ($"url" like "%backend%")
        or ($"url" like "%fullstack%")
        or ($"url" like "%cybersecurity%")
        or ($"url" like "%computer%")
        or ($"url" like "%python%")
        or ($"url" like "%java%")
        or ($"url" like "%c++%")
        or ($"url" like "%data%")
        or ($"url" like "%web%developer%")
        or ($"url" like "%web%designer%")
        or ($"url" like "%artificial%intelligence%")
        or ($"url" like "%network%")
        or ($"url" like "%programmer%")))
    .select(count($"url"))
    .show()
  }
}
