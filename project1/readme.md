### Project 1 Big Data Analysis
A big data analysis project where we answer list of questions based on wikipedia's dataset using MapReduce with the tools of Hadoop Distributed File System (HDFS), Hive, and Scala.

#### Technologies and Datasets used
- Scala 2.11, 2.13
- Scala Assembly
- Hadoop Distributed File System / Hadoop 2.7.7
- Hadoop MapReduce
- Hive 2.3.8
- DBeaver
- [Wikipedia Database](https://dumps.wikimedia.org/)

#### Questions that were answered in this project
1. Which English wikipedia article got the most traffic on January 20, 2021? [Done]
2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article?
3. What series of wikipedia articles, starting with [Hotel California](https://en.wikipedia.org/wiki/Hotel_California), keeps the largest fraction of its readers clicking on internal links?  This is similar to (2), but you should continue the analysis past the first article.  There are multiple ways you can count this fraction, be careful to be clear about the method you find most appropriate.
4. Find an example of an English wikipedia article that is relatively more popular in the Americas than elsewhere.  There is no location data associated with the wikipedia pageviews data, but there are timestamps. You'll need to make some assumptions about internet usage over the hours of the day.
5. Analyze how many users will see the average vandalized wikipedia page before the offending edit is reversed.
6. Run an analysis you find interesting on the wikipedia datasets we're using.


#### Answers / Code Snippets
Question 1
The answer for question 1 is a scala project which implements a basic MapReduce logic where it sums all the views of a pageview dataset for each article. 
In order to run this application follow these steps:
1. Uncomment out `mapReduce(args)` in the `MRDriver.scala` and comment the line `println(FileUtil.getMaxReceivedCounts("/home/jeffy892/projects/project1/project1/pageview-100+/part-r-00000"))` file located in `src/main/scala/com/revature/proj1` 
2. On the terminal, navigate do the project folder where `build.sbt` is located and type `sbt assembly` and wait until it finish.
3. Once the assembly is done, run `hadoop jar target/scala-2.13/project1-assembly-0.1.0-SNAPSHOT.jar wiki-dataset-pageviews pageviews`. This will start a Map Reduce Job.

__NOTE!!__: You will need to have the directory `wiki-dataset-pageviews` that contains all the wikipedia datasets (.tsv files) in your hdfs server.
You can do this by using the command `hdfs dfs -put <the-directory-of-pageviews>/ /wiki-dataset-pageviews`

4. Once the map reduce is over, type `hdfs dfs -get pageviews` to get the result of the MapReduce to your filesystem.

5. Once you get the result, go back to `MRDriver.scala` and comment out `mapReduce(args)` uncomment `println(FileUtil.getMaxReceivedCounts("/home/jeffy892/projects/project1/project1/pageviews/part-r-00000"))` then type and enter `sbt run` to the command line terminal.

6. It will start reading your result from the previous MapReduce and prints out the highest view article from that result.

Question 2-6
These are pretty straight forward. Just run them to your DBeaver that is connected to your hive and change the directories when loading the datasets and run them.

#### Project Slides
https://docs.google.com/presentation/d/1nTp22QkY89eHHgqS6MrZGaS7m86CF_mgCLJGELDNL2s/edit?usp=sharing

