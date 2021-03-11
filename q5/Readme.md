### Question Five (Q5)
Are there general trends in tech job postings over the past year?
What about the past month?

### Important things to Note on running the Q5 files:
We wanted to keep structure pretty flexible.
Unfortunately this means having to edit the .scala files to match up with the various queries or S3 buckets you plan on using.

To run the 3 different "types" of jobs, you'll pass in a command-line argument with the spark-submit, (1, 2, or 3) to identify what
you are running

1 - Runs createBucket.scala, and will fill the specified S3 bucket with csv files with all the data you want to query from later.  
2 - Runs countTechJobs.scala, and will count all of the URLs in the csv files that contain both "job" and one of the specified keywords,  
      and prints the total out to the console  
3 - Runs countKeywords.scala, and will count all of the URLs in the csv files that contain both "job" and one of the specified keywords,  
      and prints the total grouped by keyword to the console  
      
Each of these functions should be perfectly accessible from both a local command line input (with the proper modification) and from EMR.  

Also included at the top level is the Athena query that was run as a part of Q5 output/data gathering.  
