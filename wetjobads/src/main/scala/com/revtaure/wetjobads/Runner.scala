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
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val wetFiles = List(
      "wet/CC-MAIN-20201123153826-20201123183826-00017.warc.wet.gz",
      "wet/CC-MAIN-20201123153826-20201123183826-00018.warc.wet.gz"
      // "s3a://commoncrawl/crawl-data/CC-MAIN-2020-05/segments/1579250589560.16/wet/CC-MAIN-20200117123339-20200117151339-00018.warc.wet.gz"
      // ,"s3a://commoncrawl/crawl-data/CC-MAIN-2020-05/segments/1579250589560.16/wet/CC-MAIN-20200117123339-20200117151339-00019.warc.wet.gz"
    )

    val textInput = wetFiles.mkString(",")

    // Do some unsightly hadoop configuration in order to use a custom delimter
    val delimiter = "WARC/1.0"
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set("textinputformat.record.delimiter", delimiter)
    val hadoopFile = sc.newAPIHadoopFile(
      textInput,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      conf
    )

    val records = hadoopFile.map { case (longWritable, text) => text.toString }
    val jobAds = findJobAds(records)
    val adsWithQualifications = withQualifications(jobAds)

    adsWithQualifications.saveAsTextFile("jobAds2")
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
          (!l.startsWith("WARC") || l.startsWith("WARC-Target-URI:")) &&
            !l.startsWith("Content-Type:") &&
            !l.startsWith("Content-Length:")
        })
        textWithoutHeaders.mkString("\n")
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