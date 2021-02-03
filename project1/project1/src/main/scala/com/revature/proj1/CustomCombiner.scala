package com.revature.proj1

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class CustomCombiner extends Reducer[Text, IntWritable, Text, IntWritable] {

    override  def reduce(
            key: Text, 
            values: java.lang.Iterable[IntWritable], 
            context: Reducer[Text, IntWritable, Text, IntWritable]#Context
    ): Unit = {
        var count = 0 
        values.forEach(count += _.get())

        if(count >= 100) context.write(key, new IntWritable(count))
    }
}