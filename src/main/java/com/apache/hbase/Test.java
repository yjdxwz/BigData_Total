package com.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * 数据的添加
 * 1.的到要操作的表HTable
 * 2.创建一系列行row ------> Put
 * 3.利用HTable.put添加行
 *
 */
public class Test {
    public static void main(String[] args) throws Exception {
        //addData();
        //getData();
        //scanData();
        zhengze();
//        Configuration cfg = HBaseConfiguration.create();
//        Connection conn = ConnectionFactory.createConnection(cfg);
//        Admin admin = conn.getAdmin();
//
//        //Teacher表，添加列族binfo
//        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("Teacher"));
//        table.addFamily(new HColumnDescriptor("binfo"));
//        admin.createTable(table);
    }

    //添加一条老师的数据 名字 专业 电话 性别
    //添加一条老师的数据 名字 专业 qq 年限
    //添加一条老师的数据 名字 专业
    public static void addData() throws Exception {
        Configuration cfg = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        //Admin admin = conn.getAdmin();

        HTable table = (HTable)conn.getTable(TableName.valueOf("Teacher"));
        Put put = new Put(Bytes.toBytes("s0001"));
        put.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("王静1"));
        put.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("class"),Bytes.toBytes(".net教员"));
        put.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("telNo"),Bytes.toBytes("12341234"));
        put.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("sex"),Bytes.toBytes("男"));

        Put put1 = new Put(Bytes.toBytes("s0002"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("龙傲天1"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("class"),Bytes.toBytes("java"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("qq"),Bytes.toBytes("113113113"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("year"),Bytes.toBytes("10"));

        Put put2 = new Put(Bytes.toBytes("s0003"));
        put2.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("JoJo1"));
        put2.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("class"),Bytes.toBytes("姿势教员"));

        List<Put> list = new LinkedList<Put>();
        list.add(put);
        list.add(put1);
        list.add(put2);

        table.put(list);

        System.out.println("OK");
    }

    public static void getData() throws Exception {
        //查询，等值查询用get，模糊匹配和范围查询用scan

        //get(t0002)
        //HTable
        //Get
        //Cell
        Configuration cfg = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        //Admin admin = conn.getAdmin();
        HTable table = (HTable)conn.getTable(TableName.valueOf("Teacher"));

        Get get = new Get(Bytes.toBytes("t0002"));
        Result result = table.get(get);
        System.out.println(result);
        byte[] buff = result.getValue(Bytes.toBytes("binfo"),Bytes.toBytes("qq"));
        System.out.println(Bytes.toString(buff));

        for (Cell cell:
                result.listCells()) {
            //System.out.println(Bytes.toString(cell.getValueArray()));
            System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
    public static void scanData() throws Exception {
        //scan查询数据
        Configuration cfg = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable)conn.getTable(TableName.valueOf("Teacher"));
        Scan scan = new Scan();

        //只查询以t开头的rowkey的行
        scan.setRowPrefixFilter(Bytes.toBytes("t"));

        ResultScanner rs = table.getScanner(scan);

        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            Result result = it.next();
            //System.out.print(Bytes.toString(result.getRow()));
            for (Cell cell:
                    result.listCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.print(row+"\t"+columnName+":"+value+"\t");
            }
            System.out.println();
        }
    }

    //加入正则筛选rowkey
    public static void zhengze() throws IOException {
        Configuration cfg = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable)conn.getTable(TableName.valueOf("Teacher"));


        //创建一个过滤器容器，并设置其关系（AND/OR）
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //设置正则过滤器
        RegexStringComparator rc = new RegexStringComparator(".*2$");
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, rc);
        /*//过滤获取的条数
        Filter filterNum = new PageFilter(10);//每页展示条数*/
        //过滤器的添加
        fl.addFilter(rf);
        /*fl.addFilter(filterNum);*/
        Scan scan = new Scan();
        /*//设置取值范围
        scan.setStartRow(startRowKey.getBytes());//开始的key
        scan.setStopRow(endRowKey.getBytes());//结束的key*/
        scan.setFilter(fl);//为查询设置过滤器的list
        ResultScanner rs = table.getScanner(scan);

        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            Result result = it.next();
            //System.out.print(Bytes.toString(result.getRow()));
            for (Cell cell:
                    result.listCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.print(row+"\t"+columnName+":"+value+"\t");
            }
            System.out.println();
        }
    }
}
