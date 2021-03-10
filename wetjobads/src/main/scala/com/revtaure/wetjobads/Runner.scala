package com.revature.wetjobads

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WET Job Ads")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    sc.setLogLevel("WARN")

    val wetSegments = "s3a://wet-segments/random-paths.txt"
    val wetS3Paths = sc
      .textFile(wetSegments)
      .map("s3a://commoncrawl/" + _)
      .collect()

    val inputFiles = wetS3Paths.mkString(",")

    // Do some unsightly hadoop configuration in order to use a custom delimter
    val delimiter = "WARC/1.0"
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set("textinputformat.record.delimiter", delimiter)
    val hadoopFile = sc.newAPIHadoopFile(
      inputFiles,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      conf
    )

    val records = hadoopFile.map { case (longWritable, text) => text.toString }
    val jobAdsRdd = findJobAds(records)
    val jobAdsDf = jobAdsRdd.toDF().limit(100000)

    val outputBucket = "s3a://emr-output-revusf/jobads"
    jobAdsDf.write
      .format("csv")
      .option("compression", "gzip")
      .mode("overwrite")
      .save(outputBucket)
  }

  def findJobAds(records: RDD[String]): RDD[String] = {
    records
      .filter(record => {
        val lines = record.split("\n")
        val containsJobUri = lines
          .find(_.startsWith("WARC-Target-URI:"))
          .map(uriHeader => uriHeader.split(" "))
          .map(split => if (split.length == 2) split(1) else "")
          .map(uri => uri.contains("job"))

        containsJobUri.getOrElse(false)
      })
      .map(record => {
        val lines = record.split("\n")
        val textWithoutHeaders = lines.filter(l => {
          !l.startsWith("WARC") &&
            !l.startsWith("Content-Type:") &&
            !l.startsWith("Content-Length:") &&
            !l.trim().isEmpty()
        })
        textWithoutHeaders.mkString("")
      })
  }

  def withQualifications(jobAds: RDD[String]): RDD[String] = {
    jobAds
      .filter(ad => {
        val lowercase = ad.toLowerCase()
        lowercase.contains("requirement") ||
        lowercase.contains("skill") ||
        lowercase.contains("certification")
      })
  }
}
