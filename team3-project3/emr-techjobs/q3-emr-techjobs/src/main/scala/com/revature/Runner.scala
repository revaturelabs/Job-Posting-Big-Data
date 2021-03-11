import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._

object Runner {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("commoncrawl-emr-q3")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // val FULLINPUTBUCKETDIR = "s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-[0-9][0-9]/subset=warc/*.parquet"
    val PART_INPUTBUCKETDIR = "s3a://commoncrawl/cc-index/table/cc-main/warc/"

    val df = spark.read
      .option("header", true)
      .option("inferSchema", "true")
      .load(PART_INPUTBUCKETDIR)

    val crawl = "CC-MAIN-2020-05"
    val techJobUrls = df
      .withColumn("month", date_format(to_date($"fetch_time", "yyyy-MM-dd"), "MMMM"))
      .select("url_host_name", "url_path", "month")
      .filter($"crawl" === crawl)
      .filter($"subset" === "warc")
      .filter($"url_path".contains("/job") || $"url_path".contains("/career"))
      .filter($"url_path".rlike(".*([Ee]ngineer|[Dd]eveloper|[Pp]rogrammer|[Tt]echnology|[Ss]oftware|[Aa]nalyst|[Dd]ata|IT|[Cc]omputer|[Dd]evops|[Aa]nalytics|[Ss]oftware|[Ff]rontend|[Bb]ackend|[Ff]ullstack|[Cc]ybersecurity|[Jj]ava|[Ww]eb(-|)[Dd]eveloper|[Nn]etwork|[Aa]rtificial(-|)[Ii]ntelligence).*"))
      .limit(500000)

    val OUTPUTBUCKETDIR = "q3/commoncrawl-techjobs-data"
    val S3BUCKETDIR = s"s3a://adam-king-batch-921/${OUTPUTBUCKETDIR}"

    techJobUrls.write.mode("overwrite").format("parquet").save(S3BUCKETDIR)
    spark.close
  }
}
