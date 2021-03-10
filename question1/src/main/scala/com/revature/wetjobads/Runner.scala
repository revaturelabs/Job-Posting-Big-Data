package com.revature.wetjobads

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.functions.round 

object Runner {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder()
        .appName("WET Job Ads")
        // .master("local[4]")
        .getOrCreate()

        // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
        // Uncomment these lines to run locally, must comment out in order to run on EMR
        // EMR will provide you with keys, only need keys locally
        // val key = System.getenv(("AWS_ACCESS_KEY_ID"))
        // val secret = System.getenv(("AWS_SECRET_ACCESS_KEY"))

        // spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

        val sc = spark.sparkContext

        import spark.implicits._
        sc.setLogLevel("WARN")

        // Loading in data from a text file that contains wet.paths for the month of January 2021
        // Ending up sampling 1/10th of a months worth of data
        val wetSegments = "s3a://jan-2021-test-txt-bucket/wetTenth.txt"
        val wetS3Paths = sc
        .textFile(wetSegments)
        .map("s3a://commoncrawl/" + _)
        .collect()
    
        // textFile() will take a string of comma separated S3 URIs and read them
        // into one big RDD
        val inputFiles = wetS3Paths.mkString(",")

        val delimiter = "WARC/1.0"
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set("textinputformat.record.delimiter", delimiter)
        val hadoopFile = sc.newAPIHadoopFile(
        inputFiles,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        conf
        )

        // Read in a Census CSV gathered from https://data.census.gov/cedsci/table?q=dp05&g=0100000US.04000.001&y=2019&tid=ACSDT1Y2019.B01003&moe=false&hidePreview=true
        // This CSV was of 2019 1 year estimate for population in every state
        val censusData = spark.read
            .format("csv")
            .option("header", "true")
            .load("s3a://censusdatabucketrevature/censusdatacsv/ACSDT1Y2019.B01003_data_with_overlays_2021-02-23T105100.csv")

        // Created a State Code list for easier joining with additional warc data. 
        val rawStateList = Seq(
        ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"), ("CA", "California"), ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"), 
        ("DC", "District of Columbia"), ("FL", "Florida"), ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"), ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"), 
        ("KS", "Kansas"), ("KY", "Kentucky"), ("LA", "Louisiana"), ("ME", "Maine"), ("MD", "Maryland"), ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"), 
        ("MS", "Mississippi"), ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"), ("NH", "New Hampshire"), ("NJ", "New Jersey"), ("NM", "New Mexico"), 
        ("NY", "New York"), ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"), ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"), 
        ("SC", "South Carolina"), ("SD", "South Dakota"), ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"), ("VT", "Vermont"), ("VA", "Virginia"), ("WA", "Washington"), 
        ("WV", "West Virginia"), ("WI", "Wisconsin"), ("WY", "Wyoming"))

        val stateList = rawStateList.toDF("State Code", "State Name")

        // Combined the two dataFrames to get state codes assocaited with area name.

        val combinedCensusData = censusData.join(stateList, $"Geographic Area Name" === $"State Name")

        // combinedCensusData
        // .select("State Name", "State Code", "Population Estimate Total")
        // .show()

        val records = hadoopFile.map { case (longWritable, text) => text.toString }
        val jobAdsRdd = findJobAds(records)
        val jobAdsDf = jobAdsRdd.map(word => (word, 1)).reduceByKey(_ + _)
        val mappedLines = jobAdsDf.toDF("State Code", "Tech Job Total")
        // Joining two tables on matching States codes to get results and creating a new column for tech job abs proportional to population 
        val combinedCrawl = mappedLines.join(combinedCensusData,("State Code"))
        .withColumn("Tech Ads Proportional to Population", round(($"Tech Job Total" / $"Population Estimate Total" * 100) , 8))
        .select($"State Code", $"Geographic Area Name", $"Tech Job Total", $"Population Estimate Total", $"Tech Ads Proportional to Population")

        
        // save the results into a bukcet on s3
        // Using .repatition to save all data into one csv versus one for each loaded in segment 
        val s3OutputBucket = "s3a://commoncrawlques1outputbucket/commoncrawl-demo-data"
        combinedCrawl.repartition(1).write.format("csv").mode("overwrite").save(s3OutputBucket)
    
    }

    // Below is filtering for only Tech Jobs
    // First filter for URI's with job
    // Second fitler only returns the plain text data from the WARC headers after filtering for jobs
    // Third filter makes sure all results returned have keys words for tech jobs
    // Final filter splits all the plain text by space to prep it for finding key-value pairs
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
          !l.startsWith("WARC") &&
            !l.startsWith("Content-Type:") &&
            !l.startsWith("Content-Length:") &&
            !l.trim().isEmpty()
        })
        textWithoutHeaders.mkString("")
      }).filter(ad => {
            val lowercase = ad.toLowerCase()
            lowercase.contains("frontend") ||
            lowercase.contains("backend") ||
            lowercase.contains("fullstack") ||
            lowercase.contains("cybersecurity") ||
            lowercase.contains("software") ||
            lowercase.contains("computer")
      }).flatMap(line => line.split(" "))
      .filter(line => line.length < 3)
  }
  
}
