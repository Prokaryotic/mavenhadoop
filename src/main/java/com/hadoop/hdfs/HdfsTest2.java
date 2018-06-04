package com.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 测试hdfs2
 *
 * @author lijunxue
 * @create 2018-03-27 22:34
 **/
public class HdfsTest2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        // 这设置的是 每个文件备份几份   默认是3份  这里设置备份2份 也可以设置大于0的份数
        configuration.set("dsf.replication","2");
        FileSystem fs = FileSystem.get(configuration);

        // TODO 创建文件夹  如果你创建的文件夹 的父目录是不存在的 他会递归创建  是分大小写的
//        boolean success = fs.mkdirs(new Path("/var/hadoop/test1"));
//        if(success){
//            System.out.println("创建文件夹成功");
//        }

        // TODO 检查文件夹是否存在
//        boolean exist = fs.exists(new Path("/var/hadoop/test1"));
//        if(exist){
//            System.out.println("创建文件存在");
//        }

        // TODO 删除文件 第二个参数是 如果是文件夹是否要递归删除 如果是个文件夹 第二个参数选择了false 那么会报错
//        boolean success = fs.delete(new Path("/var/hadoop/test1"),true);
//        if(success){
//            System.out.println("删除文件成功");
//        }

        // TODO 当fs被回收的时候 删除 标记的目录 可以多次标记
//        boolean success = fs.deleteOnExit(new Path("/var/hadoop/test1"));
//        fs.deleteOnExit(new Path("/var/hadoop/test"));
//        if (success) {
//            System.out.println("删除文件成功");
//        }
//        Thread.sleep(15000);

        // TODO 是否存在该文件 或者目录
//        boolean exist = fs.exists(new Path("/var/hadoop/test"));
//        if (exist){
//            System.out.println("存在");
//        }else {
//            System.out.println("不存在");
//        }


        //  TODO 上传一个文件 第二个参数是是否覆盖
//        1.0 IOUtils.copyBytes
//        FSDataOutputStream out = fs.create(new Path("/var/hadoop/test/P_094_SHACNTXN_20170601.SQL"),true); // 这边是hdfs的目录
//        FileInputStream in = new FileInputStream("F:\\test\\P_094_SHACNTXN_20170601.SQL");
//        IOUtils.copyBytes(in,out,4096,true); // 传送完毕后时否自动关闭通道

//        2.0 自己手动写
//            FSDataOutputStream out = fs.create(new Path("/var/hadoop/test/P_094_SHACNTXN_20170601.SQL"),true); // 这边是hdfs的目录
//            FileInputStream in = new FileInputStream("F:\\test\\P_094_SHACNTXN_20170601.SQL");
//            byte [] buf = new byte[4096];
//            int temp = in.read(buf);
//            if (temp>0){
//              out.write(buf,0,temp);
//              temp = in.read(buf);
//            }
//            in.close();
//            out.close()

        // TODO 获取某个目录下的所有目录或文件的状态 只有一级 没有递归所有的目录
//        FileStatus[] statusList = fs.listStatus(new Path("/"));
//        for (int i = 0; i < statusList.length; i++) {
//            FileStatus fileStatus = statusList[i];
//            System.out.println(fileStatus.getPath()); // hdfs的路径
//            System.out.println(fileStatus.getPermission()); // 类似于linux的权限
//            System.out.println(fileStatus.getReplication()); // 这个文件备份了几次
//        }



    }
}
