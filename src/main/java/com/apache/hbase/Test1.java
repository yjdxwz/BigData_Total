package com.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Test1 {
    public static void main(String[] args)throws Exception {
        //createTable();
        //save();
        delete();
        scan();
    }
    public static void createTable() throws Exception{
        Configuration cfg = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        //Admin.createTable
        Admin admin = conn.getAdmin();
        HTableDescriptor hd= new HTableDescriptor(TableName.valueOf("teacher"));
        hd.addFamily(new HColumnDescriptor("binfo"));
        admin.createTable(hd);
        System.out.println("ok.....");
    }
    public static void save() throws Exception {
        /*
           htqaccp001  井桂印   34   大数据
           htqaccp002  蔡阳阳   女   Java教员
           htqaccp003  正仅为   篮球
         */
        Configuration cfg =HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable) conn.getTable(TableName.valueOf("teacher"));

        Put put1 = new Put(Bytes.toBytes("aaaaaaaaa"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("蔡阳阳a"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("sex"),Bytes.toBytes("女"));
        put1.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("class"),Bytes.toBytes("java教员"));

        Put put2 = new Put(Bytes.toBytes("bbbbbb"));
        put2.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("abcd"));
        put2.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("age"),Bytes.toBytes(20));
        put2.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("class"),Bytes.toBytes("uuuu"));

        Put put3 = new Put(Bytes.toBytes("benet003"));
        put3.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("name"),Bytes.toBytes("井桂印a"));
        put3.addColumn(Bytes.toBytes("binfo"),Bytes.toBytes("like"),Bytes.toBytes("篮球"));

        List<Put> puts =  new ArrayList<Put>();

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);

        table.put(puts);
        System.out.println("ok.....");
    }

    public static void scan()throws Exception{
        Configuration cfg =HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable) conn.getTable(TableName.valueOf("teacher"));

        //Get  Scan
        Scan scan = new Scan();

        //scan.setRowPrefixFilter(Bytes.toBytes("be"));
        //RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("篮球"));
       //family   qualifier
        //QualifierFilter qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("like")));
        //scan.setFilter(qf);
        //SingleColumnValueFilter scv=new SingleColumnValueFilter(Bytes.toBytes("binfo"),Bytes.toBytes("age"),CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(30)));
        //scan.setFilter(scv);
        QualifierFilter qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("age")));
        //scan.setFilter(qf);

        //Scan scan1 = new Scan(scan);
        SingleColumnValueFilter scv=new SingleColumnValueFilter(Bytes.toBytes("binfo"),Bytes.toBytes("age"),CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(20)));
        //scan1.setFilter(scv);
        List<Filter> fs = new ArrayList<Filter>();
        //fs.add(qf);
        fs.add(scv);
        //FilterListWithAND flwa= new FilterListWithAND(fs);
        FilterList fl=new FilterList();
        fl.addFilter(fs);
        scan.setFilter(fl);
        //es solr + hbase


        ResultScanner rs =  table.getScanner(scan);

        for (Result result:
             rs) {
            System.out.print(Bytes.toString(result.getRow())+"\t");
            for (Cell cell :
                    result.listCells()) {
                String columnName = null;
                String value = null;
                columnName=Bytes.toString(CellUtil.cloneQualifier(cell));
                if(columnName.equals("age")){
                    value = Bytes.toInt(CellUtil.cloneValue(cell))+"";
                }else {
                    value = Bytes.toString(CellUtil.cloneValue(cell));
                }
                String entry=columnName+":"+value;
                System.out.print(entry+"\t");
            }
            System.out.println();
        }


    }
    public static void delete() throws Exception{
        Configuration cfg =HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable) conn.getTable(TableName.valueOf("teacher"));
        Delete delete = new Delete(Bytes.toBytes("aaaaaaaaa"));
        table.delete(delete);
    }

}
