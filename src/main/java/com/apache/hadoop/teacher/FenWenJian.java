package com.apache.hadoop.teacher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FenWenJian extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"copy");
        job.setMapperClass(FenMap.class);
        job.setReducerClass(FenReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(FenPartition.class);
        job.setNumReduceTasks(4);
        //job.setSortComparatorClass(MySort.class);
        job.setGroupingComparatorClass(GroupComp.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
    }
    //hive sql
    //hive on mr  慢
    // hive on spark
    // spark rdd


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FenWenJian(),args));
        //System.out.println("a1,role1".compareTo("a1,role8"));
    }
    /**
     * 分组器
     */
    public static class GroupComp extends WritableComparator{
        public GroupComp() {
            super(Text.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String key1 = a.toString();
            String key2 = b.toString();
            String[] ss1 = key1.split(",");
            String[] ss2 = key2.split(",");
            String o1=new String(ss1[0]).trim()+new String(ss1[1]).trim();
            String o2=new String(ss2[0]).trim()+new String(ss2[1]).trim();

            return o1.compareTo(o2)!=0?o1.compareTo(o2)>0?1:-1:0;
        }
    }


    /**
     * sort 比较器
     */
    public static class MySort extends WritableComparator{
        public MySort() {
            super(Text.class,true);
        }




        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String key1 = a.toString();
            String key2 = b.toString();
           String[] ss1 = key1.split(",");
           String[] ss2 = key2.split(",");
           return -(Integer.parseInt(ss1[2])-Integer.parseInt(ss2[2]));

        }

//        @Override
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return super.compare(b1, s1, l1, b2, s2, l2);
//        }
    }

    //我要不要定义map要不要定义reduce?
    //map key LongWritable  Value: Text
    public static  class FenMap extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //String sub = value.toString().substring(0,value.toString().lastIndexOf(","));
            //context.write(new Text(sub),value);
            context.write(value,value);
        }
    }
    
    public static class FenReduce extends Reducer<Object,Text,Text,NullWritable>{
        // 保存所有的 key 利用Java collections.sort 排须，输出
        List<String> keys = new ArrayList<>();
        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] ss =key.toString().split(",");
            int sum =0;
            for (Text v :
                    values) {
                String[] ss1 = v.toString().split(",");
                int money=Integer.parseInt(ss1[2]);
                sum+=money;
                //context.write(v,NullWritable.get());

            }
            //server,role,sum
            keys.add(ss[0]+","+ss[1]+","+sum);
            //context.write(new Text(ss[0]+","+ss[1]+","+sum),NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(keys, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                        String[] ss = o1.split(",");
                        String[] ss1 = o2.split(",");
                        return  Integer.parseInt(ss1[2])-Integer.parseInt(ss[2]);
                }
            });
            for (String k :
                    keys) {
                context.write(new Text(k),NullWritable.get());
            }
        }
    }

    /**
     * 分区
     */
    public static class FenPartition extends Partitioner<Text,Text>{
        @Override
        public int getPartition(Text key, Text text, int i) {
            String line = text.toString();
            String[] ss=line.split(",");
            if("a1".equals(ss[0])){
                return 0;
            }
            if("b".equals(ss[0])){
                return 1;
            }
            if("c".equals(ss[0])){
                return 2;
            }
            if("d".equals(ss[0])){
                return 3;
            }
            return 0;
        }
    }

}
