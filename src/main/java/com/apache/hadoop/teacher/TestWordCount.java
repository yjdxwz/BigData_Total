package com.apache.hadoop.teacher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestWordCount {

    public static void main(String[] args) throws Exception{
        //编写启动code mr
        Configuration cfg = new Configuration();
        //?
        cfg.set("mapred.jar","E:\\idea_workspace\\mapreduce\\target\\mapreduce-1.0-SNAPSHOT.jar");
        Job job= Job.getInstance(cfg,"word count");
        job.setJarByClass(TestWordCount.class);
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("java1"));
        FileOutputFormat.setOutputPath(job, new Path("javao3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);







    }

    //实现map 将一个输入作为一个映射 即 key:value  这个输入就是值得文件中的一行  key:line string
    //处理value 将value部分的数据拆分成独立的单词数组
    //根据数组把数组的的每个元组做成一个映射即 key:value  key是某个单词 value是频次 1
    //i am am student  i:1 am:1 am:1 student:1
    public static class WordMap extends Mapper<Object,Text,Text,IntWritable>{
        private final Text wordKey = new Text();
        private final IntWritable one = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("[\\s+\\,\\;\\.\\[\\{\\(\\]\\}\\)\\<\\>\\=]{1,}");//拆分一行中的所有单词
            if(words!=null&&words.length>0){
                for (String word :
                        words) {
                    wordKey.set(word);
                    one.set(1);
                    context.write(wordKey,one);
                }
            }
        }
    }

    //实现reduce 缩减 聚合
    //是将map阶段产生所有k:v通过洗牌排序产生的 k:v进行处理 i:[1] am:[1,1] student:[1]
    //通过便利累加v中的值完成统计

    public static class WordReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable n :
                    values) {
                sum+=n.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }



}
