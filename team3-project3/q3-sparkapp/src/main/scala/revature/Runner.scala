import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("q3-sparkapp")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    readFromSampledData(spark)
    // readFromS3(spark)

  }

  def readFromS3(spark: SparkSession): Unit = {
    val key = System.getenv("DAS_KEY_ID")
    val secret = System.getenv("DAS_SEC")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val df = spark.read
      .format("csv")
      .load("s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-05/subset=warc/")
      
      df.createOrReplaceTempView("jobs")
      
      val techjobs = spark.sql("""SELECT url_host_name, count(*) as n, arbitrary(url_path) as sample_path
          FROM "ccindex"."ccindex"
          WHERE crawl = 'CC-MAIN-2020-05'
            AND subset = 'warc'
            AND (url_host_registry_suffix = 'com') 
            AND fetch_status = 200 
            AND content_languages = 'eng' 
            AND (LOWER(url_path) LIKE '%/job%' OR LOWER(url_path) LIKE '%/career%') 
            AND (
            LOWER(url_path) LIKE '%technology%' OR
            LOWER(url_path) LIKE '%analyst%' OR
            LOWER(url_path) LIKE '%devops%' OR
            LOWER(url_path) LIKE '%developer%' OR
            LOWER(url_path) LIKE '%frontend%' OR
            LOWER(url_path) LIKE '%backend%' OR
            LOWER(url_path) LIKE '%fullstack%' OR
            LOWER(url_path) LIKE '%cybersecurity%' OR
            LOWER(url_path) LIKE '%software%' OR
            LOWER(url_path) LIKE '%computer%' OR
            LOWER(url_path) LIKE '%python%' OR
            LOWER(url_path) LIKE '%java%' OR
            LOWER(url_path) LIKE '%c++%' OR
            LOWER(url_path) LIKE '%data%' OR 
            LOWER(url_path) LIKE '%web%developer%' OR 
            LOWER(url_path) LIKE '%web%design%' OR 
            LOWER(url_path) LIKE '%artificial%intelligence%' OR
            LOWER(url_path) LIKE '%network%' OR 
            LOWER(url_path) LIKE '%programmer%'
            )
           group by 1""").show(5000)

  }

  def readFromSampledData(spark: SparkSession): Unit = {
    val df = spark.read
      .csv("commoncrawl-sample-data/*")
      .withColumnRenamed("_c0", "url_host_name")
      .withColumnRenamed("_c1", "n")
      .withColumnRenamed("_c2", "sample_path")

    df.printSchema()
    df.show(false)

    df.createOrReplaceTempView("techjobs")

    println(
      "-----------CommonCrawl 2020 U.S Tech Jobs Data Analysis------------"
    )

    println("Total Job Posters With Tech Jobs")
    val totalJobPosters =
      spark.sql("SELECT COUNT(url_host_name) AS total_jobposters FROM techjobs")
    totalJobPosters.show(false)

    println("Count of U.S Job Posters With Less Than 4 Tech Job Posts")
    val lessThan4Posts = spark.sql(
      "SELECT count(url_host_name) AS count_less_than_4_posts FROM techjobs WHERE n < 4"
    )
    lessThan4Posts.show(false)

    println("Job Posters with Less Than 4")
    val jobPostersLessThan4 = spark.sql("SELECT url_host_name AS job_poster, n FROM techjobs WHERE n < 4")
    jobPostersLessThan4.show(false)

    println(
      "% of U.S Job Posters With Less Than 4 Tech Job Posts (Answer to Question 3)"
    )
    val percentLessThan4Posts = spark.sql(
      "SELECT (count(url_host_name) * 100 / (SELECT COUNT(url_host_name) FROM techjobs)) AS percentage_less_than_4_posts FROM techjobs WHERE n < 4"
    )
    percentLessThan4Posts.show(false)

    println("% of U.S Job Posters With More Than 4 Tech Job Posts")
    val percentGreaterThan4Posts = spark.sql(
      "SELECT (count(url_host_name) * 100 / (SELECT COUNT(url_host_name) FROM techjobs)) AS percentage_greater_than_4_posts FROM techjobs WHERE n > 3"
    )
    percentGreaterThan4Posts.show(false)
  }
}
