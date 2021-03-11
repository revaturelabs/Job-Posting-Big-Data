
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark._

object Runner {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Question6Query")
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("mode", "PERMISSIVE")
      .getOrCreate()

    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val s3Data = spark.read.option("header","false").csv("s3a://output-bucket-revusf-tyler2/crawl-data/*.csv")
    val filteredCommoncrawl = s3Data.withColumnRenamed("_c0", "filename")
    .withColumnRenamed("_c1", "tot_tech_jobs")
    .withColumnRenamed("_c2", "tot_entry_jobs")
    .withColumnRenamed("_c3", "jobs_needing_experience")
    .withColumnRenamed("_c4", "percentage")
    .filter($"tot_tech_jobs" > 0)
    
    val sums = filteredCommoncrawl.agg(sum($"tot_tech_jobs").as("tot_tech_jobs"), sum($"tot_entry_jobs").as("tot_entry_jobs"),sum($"jobs_needing_experience").as("jobs_needing_experience"))

    sums.select("tot_tech_jobs","tot_entry_jobs","jobs_needing_experience")
    .withColumn("percentage_of_entry_jobs_requiring_experience", ($"jobs_needing_experience" / $"tot_entry_jobs") * 100).show(10,false)
  }
}