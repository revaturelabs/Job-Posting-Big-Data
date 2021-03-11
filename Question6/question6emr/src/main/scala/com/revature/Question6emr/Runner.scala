package com.revature.Question6emr

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType
import com.google.flatbuffers.Struct
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import scala.io.BufferedSource
import java.io.FileInputStream
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round 
import java.util.Arrays
import java.sql
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
/**
  * Spark job ready to be run on EMR
  * Finds 500k job urls from the common crawl columnar index
  * Stores the result as a CSV file on the S3 bucket of your choosing
  */

object Runner {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("commoncrawl emr demo tyler")
      .getOrCreate()

    // Note: we're not providing any credentials or doing any s3 config here
    // EMR takes care of all of that for us

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val simpleSchema = StructType(Array(
      StructField("fileName",StringType,true),
      StructField("totalTechJob",LongType,true),
      StructField("totalEntryLevelTechJob",LongType,true),
      StructField("totalEntryLevelTechJobRequiredExperience", LongType, true),
      StructField("percentage", DoubleType, true)
    ))

    var numOfJobsRequiringExperience: Long = 0
    var numOfJobs:Long = 0
    val crawlLinks = spark.read.text("s3a://commoncrawl-project3-test-bucket-2/wet.paths2020-45.gz").collect()
    var resultsDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],simpleSchema)
    val s3OutputBucket = "s3a://output-bucket-revusf-tyler2/crawl-data"

    crawlLinks.foreach{i =>
      val fileName = i.toString().substring(1,i.toString().length()-1)
      val commonCrawl = spark.read.option("lineSep", "WARC/1.0")
      .option("mode", "PERMISSIVE")
      .text(
      s"s3a://commoncrawl/${fileName}"
    )
    .as[String]
    .map((str)=>{str.substring(str.indexOf("\n")+1)})
    .toDF("cut WET")

    val cuttingCrawl = commonCrawl
      .withColumn("_tmp", split($"cut WET", "\r\n\r\n"))
      .select($"_tmp".getItem(0).as("WARC Header"), $"_tmp".getItem(1).as("Plain Text"))

    val filterCrawl = cuttingCrawl
      .filter($"WARC Header" rlike ".*WARC-Target-URI:.*career.*" 
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*/job.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*/jobs.*")  
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*employment.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*indeed.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*dice.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*linkedin.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*glassdoor.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*cybercoders.*")
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*roberthalf.*"))
        .filter(lower($"Plain Text").contains("backend") || 
        lower($"Plain Text").contains("frontend") || 
        lower($"Plain Text").contains("front-end") || 
        lower($"Plain Text").contains("fullstack") || 
        lower($"Plain Text").contains("full stack") || 
        lower($"Plain Text").contains("cybersecurity") || 
        lower($"Plain Text").contains("cyber security") || 
        lower($"Plain Text").contains("software") || 
        lower($"Plain Text").contains("computer") || 
        lower($"Plain Text").contains("python") || 
        lower($"Plain Text").contains("java") || 
        lower($"Plain Text").contains("c++") || 
        lower($"Plain Text").contains("data") || 
        lower($"Plain Text").contains("web developer") || 
        lower($"Plain Text").contains("web design") || 
        lower($"Plain Text").contains("artificial intelligence") || 
        lower($"Plain Text").contains("network") || 
        lower($"Plain Text").contains("programmer"))
      .select($"Plain Text")
     
      val totJobs = filterCrawl.count()
      val entryJobs = filterCrawl.filter((lower($"Plain Text").contains("entry") || lower($"Plain Text").contains("junior")))
      val numOfEntryJobs = filterCrawl.filter(lower($"Plain Text").contains("entry") || lower($"Plain Text").contains("junior")).count()
      val numOfJobsRequiringExperience = entryJobs.filter(lower($"Plain Text").contains("experience")).count()
      val percentage = numOfJobsRequiringExperience.toDouble / numOfEntryJobs.toDouble
      val simpleData = Seq(Row(fileName,totJobs,numOfEntryJobs,numOfJobsRequiringExperience,percentage))

      val dfToWrite = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)
      dfToWrite.write.mode("append").csv(s3OutputBucket)
  
      
    }
    
    spark.close
  }
}
