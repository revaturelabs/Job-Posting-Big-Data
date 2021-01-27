package com.revature.proj1

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text


class CustomMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    
    /**
      * Custom Mapper that takes in a single line of string and splits based on empty space
      * The provided data is based on hourly request of users of the day.
      * To conserve time and resources I only downloaded the datas that were gathered 3 hours apart except
      * noon and after noon
      * The input follows Wikipedia's pageviews field data of: 
      * domain_code page_title count_views total_response_size
      * 
      * domain_code : This is wikipedia's "language" site versions
      * page_title : article's title
      * count_views : amount of views for this page
      * total_response_size : size of pages sent to the user (in total bytes).
      *  
      * Example:
      *     af 6_Januarie 1 0
      * 
      * @param key
      * @param value
      * @param context
      */
    override def map(
            key: LongWritable, 
            value: Text, 
            context: Mapper[LongWritable, Text, Text, IntWritable]#Context
    ): Unit = {
        val record = value.toString().split(" ")

        if(record(0) == "en") {
            //                  Article Title       count_views
            context.write(new Text(record(1)), new IntWritable(record(2).toInt))
        }
    }
}