package com.revature.proj1

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

object MRDriver {
    def main(args: Array[String]) : Unit = {
        if (args.length != 2) {
            println("Usage mr <input dir> <output dir>")
            System.exit(-1)
        }

        val job = Job.getInstance()
        
        job.setJarByClass(MRDriver.getClass())

        job.setInputFormatClass(classOf[TextInputFormat])

        // Set the file input and output based on the args passed in
        FileInputFormat.setInputPaths(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))


        job.setMapperClass(classOf[CustomMapper])
        job.setCombinerClass(classOf[CustomCombiner])
        job.setReducerClass(classOf[CustomReducer])

        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])


        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[IntWritable])

        val success = job.waitForCompletion(true) // submit configured job + wait for it to be done
        System.exit(if (success) 0 else 1) // exit with 0 if successful
        
    }
}