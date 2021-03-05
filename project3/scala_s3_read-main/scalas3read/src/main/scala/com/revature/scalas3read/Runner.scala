package com.revature.scalas3read

import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.Map
import scala.util.matching.Regex
import java.io.File
import java.io.PrintWriter
import java.time.LocalDateTime

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      //.master("local[*]")
      .getOrCreate()

    

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    //val key = System.getenv("("AWS_KEY")")
    //val secret = System.getenv(("AWS_SECRET_KEY"))

    val key = "AKIAJCL42GFQFYIBPBKA"
    val secret = ""

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\r\n\r")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val schema = new StructType()
      .add("Container", StringType, true)
      .add("Envelope", StringType, true)
      .add("_corrupt_record", StringType, true)

    val commonCrawl = spark.read
                      //.option("lineSep", "WARC/1.0")
                      //CC-MAIN-2021-04/segments/1610704847953.98/wet/CC-MAIN-20210128134124-20210128164124-00799.warc.wet.gz
                      .option("lineSep", "WARC/1.0")
                      .textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/")
  
    //commonCrawl.printSchema()
    //commonCrawl.createOrReplaceTempView("data")
    //val x = spark.sql("select Envelope.Format, Envelope.Payload-Metadata.Actual-Content-Length from temp where envelope IS NOT NULL")
    //x.show()
    //val newData = commonCrawl
    //newData.show(20, false)

    commonCrawl.printSchema()

    commonCrawl
      //.select("url_host_name", "url_path")
      //.filter($"crawl" === "CC-MAIN-2021-04")
      .filter($"value".contains("www.indeed.com") && $"value".contains("tech"))
      //.filter($"url_path".contains("career") || $"url_path".contains("software") || $"url_path".contains("developer"))
      .flatMap(extractWords(_))
      //.show(false)
      .filter($"_2".contains("tech"))
      .select("_1", "_2")
      .write
      .format("csv")
      .mode("overwrite")
      .save("s3a://bigdata-pj2-jeff/data/")

    /*
      crawl-data -> Filterm to indeed.com/glassdoor/ w/e job website -> extract the job title, and company

      -> <company, jobtitle> -> Another Filter, filters the data to tech jobs -> MapReduce/Spark

    */

  }
  //
  def extractWords(line : String) : Map[String, String] = {
    var currLine : String = ""
    currLine = line.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    currLine = currLine.split("\n\r").map(_.trim.filter(_ >= ' ')).mkString
    currLine = currLine.split('\t').map(_.trim.filter(_ >= ' ')).mkString

    var mappedTitles : Map[String, String] = Map()

    val regex = """<div class="bti-ui-job-result-detail-employer" style="">([a-zA-Z\- ]+)<\/div>""".r.findAllMatchIn(currLine)

    val regex2 = """<a target='_blank' onmousedown="indeed_clk\(this,'9861'\);" href=(["'])(.*?) class="bti-job-detail-link" id="jobURL">([a-zA-Z\- \— \/]+)<\/a>""".r.findAllMatchIn(currLine)

    while(regex.hasNext && regex2.hasNext) {
      var company = regex.next().toString.replace("""<div class="bti-ui-job-result-detail-employer" style="">""", "")
      company = company.replace("</div>", "")

      var jobTitle = regex2.next().toString().replaceFirst(
        """<a target='_blank' onmousedown="indeed_clk\(this,'9861'\);" href=(["'])(.*?) class="bti-job-detail-link" id="jobURL">""",
        ""
      )
      jobTitle = jobTitle.replace("</a>", "")

      mappedTitles += ( company -> jobTitle)
    }

    mappedTitles
  }

}