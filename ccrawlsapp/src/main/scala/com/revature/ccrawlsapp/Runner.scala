package com.revature.ccrawlsapp

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
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

    //val s3DataMaybe = spark.read.text("s3a://usf-210104-big-data/twitterstream/tweetstream-1613536993819-1")
    //val s3DataMaybe = spark.sparkContext.textFile("s3a://bigdata-pj2-teammindy/data/tweets.json")
    val s3DataMaybe = spark.sparkContext.textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704847953.98/wat/CC-MAIN-20210128134124-20210128164124-00799.warc.wat.gz")
    //s3DataMaybe.collect.foreach(println)
    //s3DataMaybe.show()
    s3DataMaybe.take(2000).foreach(println)
  }
}