package com.revature.ccrawlsapp

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", 100)
      .appName("ccrawlsapp")
      .master("local[4]")
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("AWS_ACCESS_KEY_ID"))
    val secret = System.getenv(("AWS_SECRET_ACCESS_KEY"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //val dfExpFromFile = spark.read.parquet("s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2021-04/subset=warc/part-00299-364a895c-5e5c-46bb-846e-75ec7de82b3b.c000.gz.parquet")
    //dfExpFromFile.printSchema()
    //dfExpFromFile.columns.take(2000).foreach(println)

    val dfFromFile = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    dfFromFile
    .select("url_host_name", "url_host_registered_domain", "url_host_private_domain", "url_path")
    .filter($"crawl" === "CC-MAIN-2020-16")
    .filter($"subset" === "warc")
    .filter($"url_path".rlike("job") || $"url_path".rlike("career"))
    .show(200, false)

  }
}