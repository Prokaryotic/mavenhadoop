package com.hadoop.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第一个mapreduce
 * ctrl + o 快速覆盖父类方法
 *
 * @author lijunxue
 * @create 2018-04-02 23:16
 **/
public class HelloMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] worlds = s.split(" ");
        for (int i = 0; i < worlds.length; i++) {
            String world = worlds[i];
            context.write(new Text(world),new IntWritable(1));
        }

    }
}
