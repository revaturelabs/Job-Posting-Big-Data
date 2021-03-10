package com.revature.q5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This runner file searches all csv files in a given directory,
  * and keeps a count of each url that has a specific keyword and job in it,
  * grouped by each keyword.
  * 
  * Results are written out to the console.
  */
object countKeywords {
  def start(spark: SparkSession) {

  import spark.implicits._

  val columnarIndexDF = spark
    .read
    .format("csv")
    .option("header", "true")
    // Change this line to point at a different bucket/folder in s3
    .csv("s3://path/to/your/folder/*.csv")

    columnarIndexDF
    .select("*")
    // Change this to search for a specific month or year in the csv files
    // NOTE: Common Crawl's Columnar Index data was listed by week 
    //  Example: 2020-05 is the fifth week of 2020, and the crawl for January 2020
    .filter($"crawl" like "%YYYY-WW%")
    .agg(
      // Add to/remove/edit any of these to search for different keywords
      count(when($"url" like "%software%", "url")).alias("software"),
      count(when($"url" like "%frontend%", "url")).alias("frontend"),
      count(when($"url" like "%backend%", "url")).alias("backend"),
      count(when($"url" like "%fullstack%", "url")).alias("fullstack"),
      count(when($"url" like "%cybersecurity%", "url")).alias("cybersecurity"),
      count(when($"url" like "%computer%", "url")).alias("computer"),
      count(when($"url" like "%java%", "url")).alias("java"),
      count(when($"url" like "%c++%", "url")).alias("c++"),
      count(when($"url" like "%python%", "url")).alias("python"),
      count(when($"url" like "%data%", "url")).alias("data"),
      count(when($"url" like "%web%developer%", "url")).alias("web dev"),
      count(when($"url" like "%web%designer%", "url")).alias("web design"),
      count(when($"url" like "%artificial%intelligence%", "url")).alias("AI"),
      count(when($"url" like "%network%", "url")).alias("network"),
      count(when($"url" like "%programmer%", "url")).alias("programmer")
    ).show()
  }
}
