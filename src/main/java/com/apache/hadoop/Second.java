package com.apache.hadoop;

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
import java.util.Objects;
import java.util.TreeSet;

public class Second extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job =Job.getInstance(getConf(),"second");
        job.setJarByClass(Second.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Tip.class);
        job.setMapOutputValueClass(Tip.class);
        job.setOutputKeyClass(Tip.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(MyPartition.class);
        job.setGroupingComparatorClass(GroupComp.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Second(),args));

    }

    /*
           按区求前几名
           b role1 5

           自定一个类型  Tip
                 private String server
                 private String role
                 private int money
          Writeable
          WriteableComparable

          map<Object ,Text  ,Tip ,Tip>
          line =  value.split(",")
          创建了tip对象
          k       v
          tip     tip
          reduce<Tip,Tip,Tip,NullWriteable>
          reduce
               value:[tip,tip,..]
               TreeSet
               if(TreeSet.size>topN){
                TreeSet.remove(first)
                }
          cleanup
              context.write(key,NullWriteable.get())


         Tip
            equals
            hashcode
            compareTo
            write
            readField
            toString

         Partition
            getPartition  server%4
         GroupComp

         */
    public static   class Tip implements WritableComparable<Tip>{
        private String server;
        private String role;
        private int money;
        @Override
        public int compareTo(Tip o) {
            return o.money-this.money;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tip tip = (Tip) o;
            return Objects.equals(server, tip.server) &&
                    Objects.equals(role, tip.role);
        }

        @Override
        public int hashCode() {
            return Objects.hash(server, role);
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(server);
            dataOutput.writeUTF(role);
            dataOutput.writeInt(money);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.server=dataInput.readUTF();
            this.role=dataInput.readUTF();
            this.money=dataInput.readInt();
        }

        @Override
        public String toString() {
            return server+","+role+","+money;
        }
    }
    public static class MyPartition extends Partitioner<Tip,Tip>{
        @Override
        public int getPartition(Tip tip, Tip tip2, int i) {
            if(tip.server.equals("a")){
                return 0;
            }
            if(tip.server.equals("b")){
                return 1;
            }
            if(tip.server.equals("c")){
                return 2;
            }
            if(tip.server.equals("d")){
                return 3;
            }
            return 0;
        }
    }

    public static class GroupComp implements RawComparator<Tip>{
//       private Tip key1 = new Tip();
//       private Tip key2 = new Tip();
//       private final DataInputBuffer buffer=new DataInputBuffer();
        @Override
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            //??
            return WritableComparator.compareBytes(bytes,i,i1-4,bytes1,i2,i3-4);

//            try {
//                buffer.reset(bytes,i,i1);
//                key1.readFields(buffer);
//                buffer.reset(bytes1,i2,i3);
//                key2.readFields(buffer);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            return compare(key1,key2);
        }

        @Override
        public int compare(Tip o1, Tip o2) {
            return (o1.server+o1.role).compareTo(o2.server+o2.role);
        }
    }

    public static class MyMap extends Mapper<Object,Text,Tip,Tip>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] ss = value.toString().split(",");
            if(ss!=null&&ss.length==3){
                Tip tip = new Tip();
                tip.server = ss[0];
                tip.role = ss[1];
                tip.money = Integer.parseInt(ss[2]);
                context.write(tip,tip);
            }

        }
    }
    public static class MyReduce extends Reducer<Tip,Tip,Tip,NullWritable>{

        TreeSet<Tip> sets=new TreeSet<>();
        @Override
        protected void reduce(Tip key, Iterable<Tip> values, Context context) throws IOException, InterruptedException {

            Tip t=new Tip();
            t.server=key.server;
            t.role=key.role;

            for (Tip tip:
                 values) {
                t.money+=tip.money;
            }
            sets.add(t);
            if(sets.size()>5){
                sets.remove(sets.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Tip tip :
                    sets) {
                context.write(tip,NullWritable.get());
            }
        }
    }

}
