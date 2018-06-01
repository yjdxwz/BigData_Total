package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class TestDownLoad {
    public static void main(String[] args) throws Exception {
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(cfg);
        String downloadDir = "E:\\mytext";
        OutputStream out = new FileOutputStream(new File(downloadDir, "b.java"));
        FSDataInputStream in = fs.open(new Path("java1/a.java"));
        byte[] buffer = new byte[1024];
        int n = 0;
        while (true) {
            n = in.read(buffer);
            if (n == -1) {
                break;
            }
            out.write(buffer, 0, n);
            out.flush();
        }

        System.out.println("ok......");
    }
}
