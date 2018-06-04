package com.hadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * 实验hdfs
 *
 * @author lijunxue
 * @create 2018-03-27 22:26
 **/
public class HdfsTest1 {
    public static void main(String[] args) throws Exception{
        //添加hdfs 协议的处理
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("hdfs://192.168.56.100:9000/hello.txt");
        InputStream in = url.openStream();
        // 从一个输入流里面 in  输出到一个输出流里 System.out  可以指定缓冲区大小 4096  输入流读完了之后 是否自动关闭
        IOUtils.copyBytes(in,System.out,4096,true);

    }
}
