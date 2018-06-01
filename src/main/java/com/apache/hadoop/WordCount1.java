package com.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCount1 {

    //main

    public static void main(String[] args) throws Exception {

        Configuration cfg = new Configuration();
        cfg.set("mapred.jar","E:\\idea_workspace\\demomr\\target\\demo-mr-1.0-SNAPSHOT.jar");
        Job job = Job.getInstance(cfg,"second wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("words"));
        FileOutputFormat.setOutputPath(job,new Path("result"));
        System.exit(job.waitForCompletion(true)?0:1);





    }
    //map
    public static class WordCountMapper extends Mapper<Object,Text,Text,IntWritable>{
        private static final Text word = new Text();
        private static final IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             //line 对这一行拆分 按什么拆分[ \\s+\\,\\.]
            //对数组变量制作成一个键值对 Text IntWritable
            //利用Hadoop context.write(word,one)
            String line = value.toString();
            String[] words= line.split("[\\s+\\,\\.]+");
            if(words!=null&&words.length>0){
                for (String s :
                        words) {
                    word.set(s);
                    context.write(word,one);
                }
            }

        }
    }
    //reduce
    public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable n :
                    values) {
                sum+=n.get();
            }
            context.write(key,new IntWritable(sum));

        }
    }
}
