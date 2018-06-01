package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCount {

    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
       LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] words = line.split("\t");
            for (String word:
                 words) {
                context.write(new Text(word),one);
            }
        }
    }
/**
 * 自定义 Reducel类 重写 Reduce 方法
 *
 * */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values){
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }
/**
 * WordCount  测试主方法
 *
 * */
    public static void main(String[] args) throws Exception{
        //配置参数类
        Configuration cfg = new Configuration();
        // 工作类 。工作提交
        Job job = Job.getInstance(cfg ,"word Count ");
        job.setJarByClass(WordCount.class);

        //输入
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置Mapper 和输出类型 Key - Value
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        // 设置 Reduce
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //输出
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result =  job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
