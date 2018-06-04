package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第一个reduce
 *
 * @author lijunxue
 * @create 2018-04-02 23:29
 **/
public class HelloReduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable v : values) {
             sum +=v.get();

        }
        context.write(key,new LongWritable(sum));
    }
}
