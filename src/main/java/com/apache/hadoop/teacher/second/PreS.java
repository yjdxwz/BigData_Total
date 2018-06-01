package com.apache.hadoop.teacher.second;

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

/*
上一个版本由于把jar路径还有输入输出的路径写死在代码中了，不便于更改提交环境
Hadoop util下面提供了一些工具类包我们辅助提交job
Tool  这是一个接口被ToolRunner调用
ToolRunner 这个是工具类辅助提交job
Configured 是Tool这个接口中关于configuration对象获取的实现
 */
public class PreS extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"w z");
        job.setJarByClass(PreS.class);
        job.setMapperClass(WZMapper1.class);
        job.setReducerClass(WZReduces1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int n= ToolRunner.run(new PreS(), args);
        System.exit(n);
    }
    public static class WZMapper1 extends Mapper<Object,Text,Text,IntWritable>{
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
    public static class WZReduces1 extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             /*
               key  a1 role1
               value [137,2,89]
              */
             int sum =0 ;
            for (IntWritable n :
                    values) {
                sum+=n.get();
            }
            context.write(key,new IntWritable(sum));

        }
    }

}
