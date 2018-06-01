package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.InputStream;

public class TestUpload {
    public static void main(String[] args) throws Exception {
        Configuration cfg =new Configuration();
        FileSystem fs = FileSystem.get(cfg);
        String filePath ="E:\\idea_workspace\\hdfs\\src\\main\\java\\com\\dsj2\\hdfs\\TestMKDIR.java";
        //创建输入输出流
        InputStream in = new  FileInputStream(filePath);
        byte[] buffer = new byte[1024];
        FSDataOutputStream out = fs.create(new Path("java1/b.java"));
        int n=0;
        while (true){
            n=in.read(buffer);
            if(n==-1){
                break;
            }
            out.write(buffer,0,n);
            out.hflush();
        }
        in.close();
        out.close();

        System.out.println("ok......");
    }
}
