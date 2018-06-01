package com.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Test2 extends Configured implements Tool {
    /*
    Map - reduces
    为什么会用到MR在hbase  from another hbase table to the hbase table
    去重
    Mapper inputsplit 是一个hbase table  Key rowkey  value Result
           业务逻辑 将Result做MD5，Result求hash
           做完的东西就是一个固定的值这个值做对比速度一定要快
           把它上面的值作为map的key输出，把result在作为result
    Reduce中
          支取values中的一个Result作为输出
     */
    public static void main(String[] args) throws Exception {
        //from hbase to hdfs
        //from hdfs to hbase
        //from mysql to hbase
        /*

        a.txt
           id,name,sex
        b.txt
           id,money,xueli

        c.txt
           id,name,sex,money,xueli

        思考：from hbase to hdfs
        hdfs line:  rowkey,columnName:value;columnName:value;columnName:value ..
         */
        System.exit(ToolRunner.run(new Test2(),args));

        //hive sql(创建表，导入数据，分区，查询，join)  压缩  自定义UDF
        //集群搭建  mr hive  sqoop  zookeeper  编程(自己的分布式服务的治理协调)

        //-------------------------------

        //spark  using hive  rdd.df .ds streaming

        //flume      kafka   编程   生产者   消费者


    }

//

    public int run(String[] args) throws Exception {
        //Configuration cfg = HBaseConfiguration.create(getConf());
        Job job = Job.getInstance(getConf(),"from hbase to hdfs");
        job.setJarByClass(Test2.class);
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                args[0],
                scan,
                MyMapper.class,
                Text.class,
                Text.class,
                job
        );
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true)?0:1;
    }

    /*public static class MyMapper extends Mapper<ImmutableBytesWritable, Result, Text, Text>{

    }*/
    public static class MyMapper extends TableMapper<Text,Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String line="";
            String rowkey="";
            String values="";
            rowkey = Bytes.toString(value.getRow());
            for (Cell cell:
                 value.listCells()) {
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                values +=c+":"+v+";";
            }
            line=rowkey+","+values;
            context.write(new Text(rowkey),new Text(line));
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text line = values.iterator().next();
            context.write(line,NullWritable.get());
        }
    }
}
