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
      .appName("EMR team2")
      //.master("local[*]")
      .getOrCreate()

    

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    // val key = System.getenv(("AWS_KEY"))
    // val secret = System.getenv(("AWS_SECRET_KEY"))

    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")



    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\r\n\r")

    //s3://adam-king-batch-921/testfolder/scalas3read-assembly-0.1.0-SNAPSHOT.jar
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val schema = new StructType()
      .add("Container", StringType, true)
      .add("Envelope", StringType, true)
      .add("_corrupt_record", StringType, true)

    /*
      s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/CC-MAIN-20200815184756-20200815214756-

    */
    val maxNumFiles = 2;
    val fileDir = "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/CC-MAIN-20200815184756-20200815214756-"
    val extention = ".warc.gz"
    var directory = ""
    for(i <- 0 to maxNumFiles ){

      if(i == maxNumFiles) {
        directory += (fileDir + f"$i%05d" + extention)
      }
      else {
        directory += (fileDir + f"$i%05d" + extention + ",")
      }
    }
    

    val commonCrawl = spark.read
                      //.option("lineSep", "WARC/1.0")
                      .option("lineSep", "WARC/1.0")
                      // s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00599.warc.gz
                      //s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/CC-MAIN-20210128102756-20210128132756-00738.warc.gz
                      //.textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00599.warc.gz")
                      //.textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/CC-MAIN-20210128102756-20210128132756-00738.warc.gz")
                      .textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704843561.95/warc/")
                      /*.textFile(
                        "s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00000.warc.gz",
                        "s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00001.warc.gz",
                        "s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00002.warc.gz"
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00003.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00004.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00005.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00006.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00007.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00008.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00009.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00010.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00011.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00012.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00013.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00014.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00015.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00016.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00017.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00018.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00019.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00020.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00021.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00022.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00023.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00024.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00025.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00026.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00027.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00028.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00029.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00030.warc.gz",
                        //"s3a://commoncrawl/crawl-data/CC-MAIN-2020-34/segments/1596439741154.98/warc/CC-MAIN-20200815184756-20200815214756-00031.warc.gz"
                      )*/
  
  
    commonCrawl.printSchema()

    commonCrawl
      .filter( ($"value".contains("www.indeed.com")) &&
        $"value".contains("career") || 
        $"value".contains("software") || 
        $"value".contains("developer") ||
        ($"value".contains("information") && $"value".contains("technology")) ||
        ($"value".contains("software") && $"value".contains("engineer"))
      )
      .flatMap(extractWords(_))
      .groupBy($"_1")
      .count()
      .select("_1", "count")
      .sort($"count".desc)
      //.show()
      .write
      .mode("overwrite")
      .format("csv")
      .save("s3a://bigdata-pj2-jeff/data/testresult")
      //.save("s3a://adam-king-batch-921/scalaemrdata/testresult")

    /*
      crawl-data -> Filterm to indeed.com/glassdoor/ w/e job website -> extract the job title, and company
      -> <company, jobtitle> -> Another Filter, filters the data to tech jobs -> MapReduce/Spark
    */

  }

  //Map[String, String] 
  def extractWords(line : String) : Map[String, Long] = {
    //println(currLine)
    // var currLine : String = ""
    // currLine = line.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    // currLine = currLine.split('\r').map(_.trim.filter(_ >= ' ')).mkString
    // currLine = currLine.split('\t').map(_.trim.filter(_ >= ' ')).mkString


    var mappedTitles : Map[String, Long] = Map()

    println("beggining pattern search")

    val regex = """<div.*class\s*=\s*["'].*bti-ui-job-result-detail-employer.*["']\s*>(\s*[a-zA-Z0-9\- ]+\s*)<\/div>""".r.unanchored.findAllIn(line)

    if(regex.isEmpty) {
      println("regex empty")
    }

    //val regex2 = """<a.*class\s*=\s*["'].*bti-job-detail-link.*["']\s* id="jobURL">(\s*(.*)\s*)<\/a>""".r.unanchored.findAllIn(currLine)

    // if(regex2.isEmpty) {
    //   println("regex2 empty")
    // }

    while(regex.hasNext) {
      //println(regex.next().toString + " " + regex2.next().toString)
      
      var company = regex.next().toString.replace("""<div class="bti-ui-job-result-detail-employer" style="">""", "")
      company = company.replace("</div>", "")

      // var jobTitle = regex2.next().toString().replaceFirst(
      //   """<a target='_blank' onmousedown="indeed_clk\(this,'9861'\);" href=(["'])(.*?) class="bti-job-detail-link" id="jobURL">""",
      //   ""
      // )
      // jobTitle = jobTitle.replace("</a>", "")

      

      mappedTitles += (company -> 1)
    }

    mappedTitles
  }

}