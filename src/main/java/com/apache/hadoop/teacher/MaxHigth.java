package com.apache.hadoop.teacher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MaxHigth extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"max high");
        job.setJarByClass(MaxHigth.class);
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int n= ToolRunner.run(new MaxHigth(),args);
        System.exit(n);
    }
    public static class MaxMapper extends Mapper<Object,Text,Text,IntWritable>{
        private static final Text k = new Text();
        private static final IntWritable m = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] ss = line.split(",");
            if(ss!=null&&ss.length==3){
                //a1 role1 137
                k.set(ss[0]+" "+ss[1]); //a1 role1
                m.set(Integer.parseInt(ss[2]));
                //k(a1 role1) m 137
                context.write(k,m);
            }
        }
    }
    public static class MaxReduce extends Reducer<Text,IntWritable,IntWritable,Text>{
        /*
          随着添加数据自动排序
          能够通过一端移除数
          BTree set TreeSet   TreeMap

         */
        private TreeMap<Integer,String> treeMap=new TreeMap<>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0 ;
            for (IntWritable n :
                    values) {
                sum+=n.get();
            }
            treeMap.put(sum,key.toString());
            if(treeMap.size()>10){
                treeMap.remove(treeMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            treeMap.forEach((k,v) ->{
                try {
                    context.write(new IntWritable(k),new Text(v));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            /*
            Set<Map.Entry<Integer,String>> sets = treeMap.entrySet();
            for (Map.Entry en :
                    sets) {
                context.write(new IntWritable(en.getKey()),new Text(en.getValue()));
            }
            */
        }
    }

}
