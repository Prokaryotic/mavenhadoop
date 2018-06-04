package com.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

/**
 * 主程序  这里是进行本地操作
 *
 * @author lijunxue
 * @create 2018-04-02 23:38
 **/
public class LocalMain {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration() ;
        Job job = Job.getInstance(conf);

        // 设置用哪个 map
        job.setMapperClass(HelloMap.class);
        // 设置用哪个 reduce
        job.setReducerClass(HelloReduce.class);

        // map的输出类型 如果map 和reduce的输出都一样那么 这一步不设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reduce 输出类型
        job.setOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);  //不知道为什么报错了
        job.setMapOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job, "F:\\test\\xxx\\in\\hello.txt");     // 本地上的路径
        int i = 0;
        String temp = "F:\\test\\xxx\\out\\result";
        while (new File(temp+i).exists()){
            i++;
        }
        temp +=i;

        FileOutputFormat.setOutputPath(job, new Path(temp));   // hdfs 上的路径

        job.waitForCompletion(true);
    }
}
