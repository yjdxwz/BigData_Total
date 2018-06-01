package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestMKDIR {
    public static void main(String[] args) throws Exception {

        //hdfs操作 和 java 本地文件操作的区别
        //hdfs是远程文件系统，所以操作时需要链接才能操作
        //谁告诉我链接信息 Configuration
        Configuration cfg=new Configuration(); //它默认会读取 classpath路劲下的 *-site.xml
        //创建hdfs文件系统对象
        FileSystem fs= FileSystem.get(cfg);
        //利用文件系统完成文件操作
        fs.mkdirs(new Path("java1"));

        System.out.println("ok......");
        //如果遇到权限问题 一种.添加环境变量 HADOOP_USER_NAME=hadoop
        //另一种.在运行Java程序时添加虚拟机参数 -DHADOOP_USER_NAME=hadoop


    }
}
