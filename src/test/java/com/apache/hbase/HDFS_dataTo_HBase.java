package com.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HDFS_dataTo_HBase {
    public static class MyMapper extends
            Mapper<LongWritable,Text,LongWritable,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value 就是每一行数据
            String line = value.toString();
            String [] splits = line.split("\t");
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH-mm-ss");
            String time = sdf.format(new Date(Long.parseLong(splits[0].trim())));
            String rowKey = splits[1] +"_"+ time;
            Text outValue = new Text();
            outValue.set(rowKey+"\t"+line);
            context.write(key,outValue);
        }
    }
    private static final String UTF8_ENCODING = "UTF-8";
    private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
    public static byte[] T_B(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    public static class MyReducer extends TableReducer<LongWritable,Text,NullWritable>{
        private String CF ="info";
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //
            for (Text text :
                    values) {
                String [] splits = text.toString().split("\t");
                String rowKey = splits[0];

                Put put = new Put(rowKey.getBytes());
/**
 * reportTime
 phone
 mac
 ip
 host
 siteType
 upPackNum
 downPackLoad


 httpStatus
 * */




                put.addColumn(T_B(CF),T_B("reportTime"),T_B(splits[1]));
                put.addColumn(T_B(CF),T_B("phone"),T_B(splits[2]));
                put.addColumn(T_B(CF),T_B("mac"),T_B(splits[3]));
                put.addColumn(T_B(CF),T_B("ip"),T_B(splits[4]));
                put.addColumn(T_B(CF),T_B("host"),T_B(splits[5]));
                put.addColumn(T_B(CF),T_B("siteType"),T_B(splits[6]));
                put.addColumn(T_B(CF),T_B("upPackNum"),T_B(splits[7]));
                put.addColumn(T_B(CF),T_B("downPackLoad"),T_B(splits[8]));
                put.addColumn(T_B(CF),T_B("httpStatus"),T_B(splits[9]));
               context.write(NullWritable.get(),put);
            }



        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.7.5");
        configuration.set("hbase.rootdir", "hdfs://192.168.255.130:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "192.168.255.132:2181");//指定2121 端口
        configuration.set(TableOutputFormat.OUTPUT_TABLE,args[0]);
        Job job = new Job(configuration);
        job.setJarByClass(HDFS_dataTo_HBase.class);

        //这句话很重要
        TableMapReduceUtil.addDependencyJars(job);


        job.setMapperClass(MyMapper.class);

        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job,args[1]);

        job.setOutputFormatClass(TableOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
