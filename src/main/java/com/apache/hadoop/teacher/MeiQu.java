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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

public class MeiQu extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new MeiQu(),args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"mei qu");
        job.setJarByClass(MeiQu.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        job.setPartitionerClass(MyPartition.class);
        job.setMapOutputKeyClass(Tip.class);
        job.setMapOutputValueClass(Tip.class);
        job.setOutputKeyClass(Tip.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;

    }

    /**
     * 我的自定义类型如何序列化
     */
    public static class Tip implements WritableComparable<Tip> {
        private String server;
        private String role;
        private int money;

        @Override
        public String toString() {
            return server+","+role+","+money;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Tip){
                Tip tip = (Tip) obj;
                return this.server.equals(tip.server)&&this.role.equals(tip.role);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public int compareTo(Tip o) {
            return o.money-this.money;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(server);
            dataOutput.writeUTF(role);
            dataOutput.writeInt(money);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
           this.server = dataInput.readUTF();
           this.role = dataInput.readUTF();
           this.money = dataInput.readInt();
        }
    }
    /*
       map > k Tip v Tip
       reduce  4
           Tip <=>[Tip,Tip,.......]
     */

    public static class MyPartition extends Partitioner<Tip,Tip>{
        @Override
        public int getPartition(Tip k, Tip v, int reduceTaskNum) {
            return k.server.hashCode()%reduceTaskNum;
        }
    }
    public static class MyMapper extends Mapper<Object,Text,Tip,Tip>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] ss = line.split(",");
            if(ss!=null&&ss.length==3){
                Tip tip=new Tip();
                tip.server=ss[0];
                tip.role=ss[1];
                tip.money=Integer.parseInt(ss[2]);
                context.write(tip,tip);
            }
        }
    }
    public static class MyReduce extends Reducer<Tip,Tip,Tip,NullWritable>{
        private TreeSet<Tip> set= new TreeSet<>();
        @Override
        protected void reduce(Tip key, Iterable<Tip> values, Context context) throws IOException, InterruptedException {
            for (Tip n :
                    values) {
                key.money+=n.money;
            }
            set.add(key);
            int topN=Integer.parseInt(context.getConfiguration().get("topN","3"));
            if(set.size()>topN){
                set.remove(set.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Tip tip :
                    set) {
                context.write(tip,NullWritable.get());
            }
        }
    }

}
