package com.hks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MkDirDemo {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 获得FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.70.3:9000"), new Configuration(), "root");
        // 调用mkdirs方法，在HDFS文件服务器上创建文件夹。
        boolean flag = fileSystem.mkdirs(new Path("/hadoop/dir3"));
        // 执行结果：true
        System.out.println(flag);
    }

}
