package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 这使用了yarn 直接在hdfs 上进行 mapreduce 计算
 *
 * @author lijunxue
 * @create 2018-04-03 21:53
 **/
public class YarnMain {

    public static void main(String[] args) throws Exception {
        //
        Configuration conf = new Configuration();
        // hdfs
        conf.set("fs.defaultFS", "hdfs://master:9000/");

        // 设置 jar的路径
        conf.set("mapreduce.job.jar","E:\\work\\wb\\mavenhadoop\\target\\mavenhadoop-1.0-SNAPSHOT.jar");
        // 设置yarn的配置 注意查看 yarn和hdfs是否都开启
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");

        // 如果没有设置 将会导致 一直连接 默认的0.0.0.0:10020 然后报连接不成功
        conf.set("mapreduce.jobhistory.address", "master:10020");
        // 没有这个就会出现
        conf.set("mapreduce.app-submission.cross-platform", "true");

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
        // job.setMapOutputValueClass(LongWritable.class);  //不知道为什么报错了
        job.setMapOutputValueClass(IntWritable.class);


        // 这里可以指某个目录下的所有文件都做这种处理
        FileInputFormat.setInputPaths(job, "hdfs://master:9000/var/hadoop/test1/in/");
        // 如果配置了 conf.set("fs.defaultFS","hdfs://192.168.56.100:9000"); 直接可以用 /var/hadoop/test1/in/

        // 输出文件夹必须是没有被创建的 不然的话会报错
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/var/hadoop/test1/out/res18/"));
        // 如果配置了 conf.set("fs.defaultFS","hdfs://192.168.56.100:9000"); 直接可以用 /var/hadoop/test1/out/res2/
        job.waitForCompletion(true);
    }
}
