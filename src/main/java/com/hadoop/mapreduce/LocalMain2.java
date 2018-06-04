package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

/**
 * hsfs 上拉东西过来 mapreduce 还是在本地上运行
 *
 * @author lijunxue
 * @create 2018-04-03 20:58
 **/
public class LocalMain2 {
    public static void main(String[] args) throws Exception{
        // 因为这里没有配置 conf 的配置文件 所以所有的文件都是利用 hdfs 从 hdfs的输入目录 把 文件传送到 本地 然后 在把输出文件 传输到 hdfs 的输出目录
        // 这里没有用到 yarn
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




        // 这里可以指某个目录下的所有文件都做这种处理
        FileInputFormat.setInputPaths(job, "hdfs://192.168.56.100:9000/var/hadoop/test1/in/");     // 本地上的路径

        // 输出文件夹必须是没有被创建的 不然的话会报错
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.56.100:9000/var/hadoop/test1/out/res1/"));   // hdfs 上的路径

        job.waitForCompletion(true);
    }
}
