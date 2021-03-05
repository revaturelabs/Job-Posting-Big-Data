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

object GetS3Data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Get S3 Data")
      .config("spark.master", "local[4]")
      .config("spark.sql.warehouse.dir", "src/main/recources/warehouse")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.WARN)

    val key = System.getenv(("AWS_ACCESS_KEY"))
    val secret = System.getenv(("AWS_SECRET_KEY"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.attempts.maximum", "30")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)

    //rddParser(spark)
    //dfParser(spark)
    //urlIndex(spark)
    jobExample(spark)

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

    val commonCrawlJobs = spark.read
      .option("lineSep", "WARC/1.0")
      .parquet(
        "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/wet/"
      )
      .as[String]
      .map(str => str.substring(str.indexOf("\n") + 1))
      .filter($"value" rlike jobsRegex)

    val commonCrawlTechJobs = commonCrawlJobs
      .filter($"value" rlike techJobs.mkString("|"))

    commonCrawlTechJobs.show(2, false)

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

    val df = spark.sparkContext.textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/wet.paths.gz").foreach(println)
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
