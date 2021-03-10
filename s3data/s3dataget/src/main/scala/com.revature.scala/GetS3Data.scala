package `com.revature.scala`

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  BooleanType,
  StringType
}
import org.apache.spark.sql.Dataset

object GetS3Data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Get S3 Data")
      //.config("spark.master", "local[*]")
      .config("spark.sql.warehouse.dir", "src/main/recources/warehouse")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.WARN)

    val key = System.getenv(("AWS_ACCESS_KEY"))
    val secret = System.getenv(("AWS_SECRET_KEY"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)

    //rddParser(spark)
    dfParser(spark)
    //urlIndex(spark)
    //jobExample(spark)

    spark.close()
  }

  def rddParser(spark: SparkSession): Unit = {

    val rdd = spark.sparkContext.textFile(
      "s3a://commoncrawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz"
    )

    //rdd.take(2000).foreach(println)

    val flatRDD =
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).filter(_._2 > 5)

    flatRDD.take(2000).foreach(println)

    // rdd
    // .filter(line => line.contains("a href") && (line.contains("/job") || line.contains("/jobs") || line.contains("/job-listing")))
    // .take(200)
    // .distinct
    // .foreach(println)

    // val parsedRDD = rdd
    //   .flatMap(line =>
    //     line.split("""\s+""") match {
    //       case Array(href, _) => Some(href)
    //     }
    //   )
  }

  def dfParser(spark: SparkSession): Unit = {

    import spark.implicits._
    // val df = spark.read
    //   .format("json")
    //   .options(
    //     Map(
    //       "compression" -> "gzip",
    //       "inferSchema" -> "true",
    //       "mode" -> "dropMalformed",
    //       "lineSep" -> """\r\n\r\n""",
    //       "path" -> "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610704847953.98/wet/CC-MAIN-20210128134124-20210128164124-00799.warc.wet.gz"
    //     )
    //   )
    //   .load()

    val jobsRegex = List(
      "jobs",
      "job-listing",
      "job-posting",
      "indeed.com/",
      "careers",
      "glassdoor.com/",
      "/employment"
    )

    val techJobs = List(
      "technology",
      "comput",
      "java",
      "python",
      "scala",
      "code",
      "coding",
      "programming",
      "backend",
      "frontend",
      "web-development",
      "website-development",
      "ruby",
      "sql",
      "html",
      "fullstack",
      "full-stack",
      "css",
      "software",
      "cybersecurity",
      "cryptography",
      "it-support",
      "it-specialist",
      "spark",
      "hive",
      "hql",
      "hadoop",
      "mapreduce",
      "hdfs",
      "c#",
      "sdk",
      "aws",
      "data",
      "apache",
      "kafka",
      "mongo",
      "c#",
      "programmer",
      "analytics"
    )

    val questionFilter = "low cod|no cod|low-cod|no-cod"

    // lazy val commonCrawl = spark.read
    //   .option("lineSep", "WARC/1.0")
    //   .textFile(
    //     "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wet/CC-MAIN-20210115134101-20210115164101-0018*.warc.wet.gz"
    //   )
    //   .map(str => str.substring(str.indexOf("\n") + 1))
    //   .withColumn("Header", split($"value", "\r\n\r\n").getItem(0))
    //   .withColumn("Content", split($"value", "\r\n\r\n").getItem(1))
    //   .drop("value")
    //   .coalesce(20)

    // lazy val englishJobSites = commonCrawl
    //   .filter(
    //     $"Header" rlike ".*WARC-Target-URI:.*careers.*"
    //       or ($"Header" rlike ".*WARC-Target-URI:.*job-listing.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*jobs.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*employment.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*indeed/.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*job-posting.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*glassdoor/.*")
    //       and ($"Header" contains "WARC-Identified-Content-Language: eng" and !($"Header" contains ","))
    //   )
    //   .coalesce(10)
    //   .cache()

    // lazy val techJobSites = englishJobSites
    //   .filter(
    //     $"Header" rlike ".*WARC-Target-URI:.*/jdk.*"
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/technology.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/comput.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/java.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/python.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/scala.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/code.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/coding.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/programming.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/backend.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/frontend.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/webdevelopment.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/web-development.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/websitedevelopment.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/website-development.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/ruby.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/sql.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/html.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/fullstack.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/full-stack.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/css.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/software.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/cyber.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/crypto.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/itsupport.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/it-support.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/itspecialist.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/it-specialist.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/spark.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/hive.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/hql.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/hadoop.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/apache.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/mapreduce.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/hdfs.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/kafka.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/cassandra.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/mongo.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/programmer.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/programming.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/aws.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/athena.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/emr.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/s3.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/cloud.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/analytics.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/sdk.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/jvm.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/jre.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/byte.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/visual-studio.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/eclipse.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/intellij.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/visualstudio.*")
    //       or ($"Header" rlike ".*WARC-Target-URI:.*/vsc.*")
    //   )
    //   .coalesce(8)
    //   .cache()

    // techJobSites.show(5, false)

    // lazy val lowCodeJobs = techJobSites
    //   .filter(lower($"Content") rlike questionFilter)
    //   .coalesce(4)
    //   .cache()

    // lazy val jobCount = englishJobSites.count.toDouble

    // println(s"The total number of job related websites in the Common Crawl database is: $jobCount")

    // lazy val techCount = techJobSites.count.toDouble

    // println(s"The total number of tech related websites in the Common Crawl database is: $techCount")

    // lazy val lowCodeCount = lowCodeJobs.count.toDouble

    // println(f"The total number of low code websites in the Common Crawl database is: $lowCodeCount")

    // lazy val techJobPercent = techCount / jobCount * 100

    // println(f"The percentage of tech jobs to total jobs in the Common Crawl database is: $techJobPercent%.4f%%")

    // lazy val lowCodePercent = lowCodeCount / techCount * 100

    // println(f"The percentage of low code jobs to tech jobs is: $lowCodePercent%.4f%%")

    // lazy val techJobSitesWide = englishJobSites
    //   .filter(
    //     $"Content" contains "jdk"
    //       or ($"Content" contains "technology")
    //       or ($"Content" contains "comput")
    //       or ($"Content" contains "java")
    //       or ($"Content" contains "python")
    //       or ($"Content" contains "scala")
    //       or ($"Content" contains "code")
    //       or ($"Content" contains "coding")
    //       or ($"Content" contains "programming")
    //       or ($"Content" contains "backend")
    //       or ($"Content" contains "frontend")
    //       or ($"Content" contains "webdevelopment")
    //       or ($"Content" contains "web development")
    //       or ($"Content" contains "websitedevelopment")
    //       or ($"Content" contains "website development")
    //       or ($"Content" contains "ruby")
    //       or ($"Content" contains "sql")
    //       or ($"Content" contains "html")
    //       or ($"Content" contains "fullstack")
    //       or ($"Content" contains "full stack")
    //       or ($"Content" contains "css")
    //       or ($"Content" contains "software")
    //       or ($"Content" contains "cyber")
    //       or ($"Content" contains "crypto")
    //       or ($"Content" contains "itsupport")
    //       or ($"Content" contains "it support")
    //       or ($"Content" contains "itspecialist")
    //       or ($"Content" contains "it specialist")
    //       or ($"Content" contains "spark")
    //       or ($"Content" contains "hive")
    //       or ($"Content" contains "hql")
    //       or ($"Content" contains "hadoop")
    //       or ($"Content" contains "apache")
    //       or ($"Content" contains "mapreduce")
    //       or ($"Content" contains "hdfs")
    //       or ($"Content" contains "kafka")
    //       or ($"Content" contains "cassandra")
    //       or ($"Content" contains "mongo")
    //       or ($"Content" contains "programmer")
    //       or ($"Content" contains "programming")
    //       or ($"Content" contains "aws")
    //       or ($"Content" contains "athena")
    //       or ($"Content" contains "emr")
    //       or ($"Content" contains "s3")
    //       or ($"Content" contains "cloud")
    //       or ($"Content" contains "analytics")
    //       or ($"Content" contains "sdk")
    //       or ($"Content" contains "jvm")
    //       or ($"Content" contains "jre")
    //       or ($"Content" contains "byte")
    //       or ($"Content" contains "visual studio")
    //       or ($"Content" contains "eclipse")
    //       or ($"Content" contains "intellij")
    //       or ($"Content" contains "visualstudio")
    //       or ($"Content" contains "vsc")
    //   )
    //   .repartition(2)
    //   .cache()

    // lazy val lowCodeWide = techJobSitesWide
    //   .filter(lower($"Content") rlike questionFilter)
    //   .coalesce(4)
    //   .cache()

    // lazy val totalJobSiteCount = englishJobSites.count.toDouble

    // println(s"The total number of job hiring websites in the Common Crawl database is: $totalJobSiteCount")

    // lazy val techJobCountWide = techJobSitesWide.count.toDouble

    // println(s"The total number of tech related job postings in the Common Crawl database is: $techJobCountWide")

    // lazy val lowCodeJobCountWide = lowCodeWide.count.toDouble

    // println(s"The total number of low code required tech job postings in the Common Crawl database is $lowCodeJobCountWide")

    // lazy val techJobSitePercent = techJobCountWide / totalJobSiteCount * 100

    // println(f"The percentage of job postings that are tech related in the Common Crawl database is: $techJobSitePercent%.3f%%")

    // lazy val lowCodeJobPercent = lowCodeJobCountWide / techJobCountWide * 100

    // println(f"The percentage of tech job postings that require low or no code in the Common Crawl database is: $lowCodeJobPercent%.3f%%")

    lazy val text = spark.read.text(
      "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wet/CC-MAIN-20210115134101-20210115164101-00185.warc.wet.gz"
    )

    lazy val words = text.select(split($"value", " ").as("Words"))

    lazy val wordsSingle = words.select(explode($"Words").as("Words"))

    lazy val stringWords =
      wordsSingle.select($"Words".cast(StringType).as("languages"))

    lazy val lowerWords =
      stringWords.select(lower($"languages").as("languages"))

    lazy val languagesFilter = List(
      "^python$",
      "^java$",
      "^scala$",
      "^javascript$",
      "^ruby$",
      "^sql$",
      "^hql$",
      "^kotlin$",
      "^php$",
      "^perl$",
      "^matlab$",
      "^typescript$",
      "^objective-c$"
    )

    var languages = lowerWords
      .filter(
        lower($"languages") rlike languagesFilter.mkString("|") or lower(
          $"languages"
        ) === "c++" or lower($"languages") === "c#"
      )
      .groupBy($"languages")
      .pivot($"languages")
      .count
      .drop("languages")

    languages.show(false)

    val warcSchema = StructType(
      Array(
        StructField(
          "Container",
          StructType(
            Array(
              StructField("Compressed", BooleanType, nullable = true),
              StructField("Filename", StringType, nullable = true),
              StructField(
                "Gzip-Metadata",
                StructType(
                  Array(
                    StructField("Deflate-Length", StringType, nullable = true),
                    StructField("Footer-Length", StringType, nullable = true),
                    StructField("Header-Length", StringType, nullable = true),
                    StructField("Inflated-CRC", StringType, nullable = true),
                    StructField("Inflated-Length", StringType, nullable = true)
                  )
                )
              )
            )
          )
        )
      )
    )

    val testSchema = StructType(
      Array(StructField("WARC-Target-URI", StringType, nullable = true))
    )

    case class WARC(
        Compressed: Boolean
    )

    // val df = spark.read
    //   .format("text")
    //   .options(
    //     Map(
    //       "compression" -> "gzip",
    //       "mode" -> "dropMalformed",
    //       "multiline" -> "true",
    //       "encoding" -> "UTF-16LE",
    //       "path" -> "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/warc/CC-MAIN-20210115134101-20210115164101-00015.warc.gz",
    //       "inferSchema" -> "true"
    //     )
    //   )
    //   .load()

    // val splitDF =
    //   df.select(split($"value", "</html>").as("Websites")).drop("value")

    // val dropDFs = df
    //   .drop(
    //     ($"Container") and ($"Envelope.Format") and ($"Envelope.Payload-Metadata.Actual-Content-Length")
    //     and ($"Envelope.Payload-Metadata.Actual-Content-Type") and ($"Envelope.Payload-Metadata.Block-Digest")
    //     and ($"Envelope.Payload-Metadata.HTTP-Request-Metadata.Entity-Digest") and ($"Envelope.Payload-Metadata.HTTP-Request-Metadata.Entity-Length")
    //     and ($"Envelope.Payload-Metadata.HTTP-Request-Metadata.Entity-Trailing-Slop-Length")
    //   )
    //   .filter(
    //     $"Envelope.Payload-Metadata.HTTP-Request-Metadata.Headers.Accept-Language" contains "en-US"
    //   )

    // val parsedDF = df
    //   .select("Envelope.WARC-Header-Metadata")
    //   .filter(($"Envelope.Payload-Metadata.HTTP-Request-Metadata.Headers.Accept-Language" contains "en-US")
    //   and ($"Envelope.WARC-Header-Metadata.WARC-Target-URI" contains "/job-listing"))

    // parsedDF.show(200, false)

  }

  def urlIndex(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read
      .format("parquet")
      .options(
        Map(
          "compression" -> "gzip",
          "mode" -> "dropMalformed",
          "inferSchema" -> "true",
          "path" -> "s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2021-04/subset=warc/"
        )
      )
      .load()

    val jobsRegex = "/jobs|/job-listing|/job-posting"

    val techJobs = List(
      "/technology",
      "/computer",
      "/java",
      "/python",
      "/scala",
      "/code",
      "/coding",
      "/programming",
      "/backend",
      "/frontend",
      "/web-development",
      "/website-development",
      "/ruby",
      "/sql",
      "/html",
      "/fullstack",
      "/full-stack",
      "/css",
      "/software",
      "/cybersecurity",
      "/cryptography",
      "/it-support",
      "/it-specialist",
      "/spark",
      "/hive",
      "/hql",
      "/hadoop",
      "/mapreduce",
      "/hdfs",
      "/c#",
      "/sdk",
      "/aws",
      "/computing",
      "/data",
      "/apache",
      "/kafka",
      "/mongo"
    )

    val jobSiteIndex = df
      .filter(
        ($"content_languages" === "eng") and ($"content_charset" === "UTF-8") and (($"url_path"
          .rlike(jobsRegex)) and ($"url_path".rlike(techJobs.mkString("|"))))
      )
      .select(
        $"url",
        $"warc_filename",
        $"warc_record_offset",
        $"warc_record_length"
      )

    //jobSiteIndex.show(jobSiteIndex.count.toInt, false)

    jobSiteIndex
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Append)
      .save("TechJobsiteIndex")

    // val df = spark.read
    //   .format("json")
    //   .options(
    //     Map(
    //       "header" -> "false",
    //       "mode" -> "dropMalformed",
    //       "inferSchema" -> "true",
    //       "path" -> "s3a://commoncrawl/cc-index/table/cc-main/warc/"
    //     )
    //   )
    //   .load()

    // df.select()

    //   val techJobsDF = df
    //     .filter(
    //       $"_c0" contains "/jobs" and $"_c0" contains
    //       "tech|tech|computer|computer|cryptograpy|end|full|java|python|scala|spark|sql|C+|C#|unix"
    //     )
    //     .withColumnRenamed("_c0", "URI")
    //     .withColumnRenamed("_c1", "Path")
  }

  def jobExample(spark: SparkSession): Unit = {

    import spark.implicits._
    // val df = spark.read
    //   .format("parquet")
    //   .options(
    //     Map(
    //       "mode" -> "dropMalformed",
    //       "inferSchema" -> "true",
    //       "compression" -> "gzip",
    //       "path" -> "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/wet.paths.gz"
    //     )
    //   )
    //   .load()

    // val exampleFormat = df
    //   .filter(
    //     ($"url_path" contains "jobs") and ($"content_languages" === "eng")
    //   )
    //   .select($"url_host_name", $"url_path" as "sample_path")
    //   .groupBy("url_host_name", "sample_path")
    //   .count()
    //   .orderBy($"count" desc)
    //   .as("n")

    // exampleFormat.show(200, false)

    val rdd = spark.sparkContext
      .textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/wet.paths.gz")
      .coalesce(1, true)
      .saveAsTextFile("WETfiles")

  }
}

/** val regexSting = "volkswagen|vw"
  * val vwDF = carsDF.select(
  *   col("Name"),
  *   regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  * ).where(col("regex_extract") =!= "").drop("regex_extract")
  *
  * vwDF.select(
  *   col("Name"),
  *   regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  * .show())
  */
