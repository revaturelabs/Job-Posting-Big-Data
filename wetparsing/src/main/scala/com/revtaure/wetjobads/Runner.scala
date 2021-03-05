package com.revature.wetjobads

import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("project3")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.mapreduce.Job
    import org.apache.hadoop.io.{LongWritable, Text}
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

    val file = "CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet"
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set("textinputformat.record.delimiter", "WARC/1.0")
    val input = sc.newAPIHadoopFile(
      file,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      conf
    )

    val lines = input.map { case (_, text) => text.toString }
    lines.take(200).foreach(println)

  }
}
