package com.revature.q5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This file is the routine used to create a group of
  * csv files in S3 for querying purposes via EMR.
  */
object createBucket {
  def start(spark: SparkSession) {

    import spark.implicits._

    // Loads data from Common Crawl's Columnar Index;
    // Change if reading data from elsewhere.
    val df = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    // Change year to specify a different subset of data
    val crawl = "CC-MAIN-YYYY%"
    val jobUrls = df
      .select((lower($"url_path")).as("url"), $"crawl")
      .filter($"crawl" like crawl)
      .filter($"subset" === "warc")
      .filter($"fetch_status" === 200)
      .filter($"url".like("%job%"))
      .filter(
        // Add or remove keywords that might appear in the URL as necessary
        ($"url" like "%software%") or 
        ($"url" like "%frontend%") or
        ($"url" like "%backend%") or
        ($"url" like "%fullstack%") or
        ($"url" like "%cybersecurity%") or
        ($"url" like "%computer%") or
        ($"url" like "%java%") or
        ($"url" like "%python%") or
        ($"url" like "%c++%") or
        ($"url" like "%data%") or
        ($"url" like "%web%developer%") or
        ($"url" like "%web%designer%") or
        ($"url" like "%artificial%intelligence%") or
        ($"url" like "%network%") or
        ($"url" like "%programmer%")
      )

  // Designates where the output files will be written.
  val s3OutputBucket = "s3://path/to/your/folder"

  jobUrls.write.format("csv").option("header", "true").mode("overwrite").save(s3OutputBucket)

  spark.close
  }
}