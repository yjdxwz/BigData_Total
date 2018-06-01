package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class Hadoop_Api {
    FileSystem fileSystem = null;
    Configuration configuration = null;

    public static final String hadoop_FilePath = "hdfs://nn:9000";

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(hadoop_FilePath), configuration, "hadoop1");
        System.out.println("setUp() is Started !!!!!!!");
    }

    @After
    public void End_up() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("End_up is Starting !!!!!!!!");
    }

    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/npb_Test/aaa"));
    }

    @Test
    public void Add_file() throws Exception {
        if (fileSystem.delete(new Path("/ssss"))) {
            System.out.println("递归删除成功");
        } else {
            System.out.println("递归删除失败");
        }
    }
    @Test
    public void Create_file() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/input/ssaa.txt"), true);
        output.write("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww".getBytes());
        output.flush();
        output.close();
    }
    @Test
    public void cat_file() throws Exception {
        FSDataInputStream output = fileSystem.open(new Path("/input/caonimazhangruijie.txt"));
        IOUtils.copyBytes(output, System.out, 1024);
    }
    @Test
    public void rename() throws Exception {

       // EmbeddedThriftBinaryCLIService
        boolean flag = fileSystem.rename(new Path("/input/aaa.txt"), new Path("/input/aassssa.txt"));
        if (flag)
            System.out.print("删除成功");
        else
            System.out.print("删除不成功");
    }
    @Test
    public void copyFromLocal() throws Exception {

        fileSystem.copyFromLocalFile(new Path("D:\\MyWindow\\Music\\Exception.txt"), new Path("/input/caonimazhangruijie.txt"));
    }
    @Test
    public void copyFromLocalWithProgress() throws Exception {
        InputStream input = new BufferedInputStream(new FileInputStream(new File("D:\\Resource\\SoftWare\\apache-hive-2.3.2-bin.tar.gz")));
        Progressable progressable = new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        };
        FSDataOutputStream output = fileSystem.create(new Path("/input/aaaaaaa.tar.gz"), progressable);
        IOUtils.copyBytes(input, output, 4096);


    }

}
