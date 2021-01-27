package com.revature.proj1

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

class CustomReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(
        key: Text, 
        values: java.lang.Iterable[IntWritable], 
        context: Reducer[Text, IntWritable, Text, IntWritable]#Context
    ): Unit = {
        // Only check the articles that were visted more than 20 times
        val iter = values.iterator()
        var max = 0
        var maxKey : String = ""
        while(iter.hasNext()) {
            var value = iter.next().toString
            if(value.toInt > max) {
                max = value.toInt
                maxKey = key.toString
            }
        }
        context.write(new Text(maxKey), new IntWritable(max))
        
    }
}