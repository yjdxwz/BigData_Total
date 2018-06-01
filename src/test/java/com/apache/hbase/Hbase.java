package com.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class Hbase {
    /**
     * 连接 Hbase 的 接口
     */
    Connection connection = null;
    /**
     * 用于和Hbase单个表交互
     * 可以使用get, put, delete or scan data
     */
    Table table = null;
    /**
     * HBase的管理API。
     * Admin可用于创建、删除、列表、启用和禁用表、添加和删除表。
     * 列家庭及其他行政运作。
     */
    Admin admin = null;

    /**
     * 开始之前 创建配置信息
     */
    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.7.5");
        configuration.set("hbase.rootdir", "hdfs://192.168.255.130:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "192.168.255.132:2181");//:2181
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

    }

    /**
     * 结束之后 关闭对象
     */
    @After
    public void End_up() throws Exception {
        if (connection != null) connection.close();
    }

    /**
     * 创建表
     */
    @Test
    public void Create_Table() throws Exception {
        String table_name = "school";
        if (admin.tableExists(TableName.valueOf("table_name"))) {
            System.out.println("表已经存在");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table_name));
            //添加列族columnFamily ，不必指定列
            hTableDescriptor.addFamily(new HColumnDescriptor("address"));
            hTableDescriptor.addFamily(new HColumnDescriptor("more"));
            hTableDescriptor.addFamily(new HColumnDescriptor("info"));
            admin.createTable(hTableDescriptor);
            System.out.println("表" + table_name + "创建成功");
        }
    }

    /**
     * 查询表
     **/
    @Test
    public void ShowTable_Data() throws Exception {
        HTableDescriptor[] listTables = admin.listTables();
        if (listTables.length > 0) {
            for (HTableDescriptor ta : listTables) {
                System.out.println(ta.getNameAsString());
                for (HColumnDescriptor column :
                        ta.getColumnFamilies()) {
                    System.out.println("\t" + column.getNameAsString());
                }
            }
        }
    }

    /**
     * 添加多条数据
     * 添加单条数据 传入参数换成put 对象就可以了
     * table.put(Put put);
     */
    @Test
    public void Put_table() throws Exception {

        Table table = connection.getTable(TableName.valueOf("school"));
        //创建 put，并制定 put 的Rowkey
        Put put = new Put(Bytes.toBytes("zhangsan"));
        //byte [] family, byte [] qualifier, byte [] value
        put.addColumn(Bytes.toBytes("student"), Bytes.toBytes("age"), Bytes.toBytes(18));
        put.addColumn(Bytes.toBytes("student"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes("student"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
        put.addColumn(Bytes.toBytes("student"), Bytes.toBytes("address"), Bytes.toBytes("henan"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hobby"), Bytes.toBytes("girl"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("good"), Bytes.toBytes("basketball"));

        Put put1 = new Put(Bytes.toBytes("lisi"));
        // family ,qualifier, value  顺序不可乱，列族，列，内容
        put1.addColumn(Bytes.toBytes("student"), Bytes.toBytes("age"), Bytes.toBytes(22));
        put1.addColumn(Bytes.toBytes("student"), Bytes.toBytes("sex"), Bytes.toBytes("女"));
        put1.addColumn(Bytes.toBytes("student"), Bytes.toBytes("city"), Bytes.toBytes("nanjing"));
        put1.addColumn(Bytes.toBytes("student"), Bytes.toBytes("address"), Bytes.toBytes("hebei"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hobby"), Bytes.toBytes("boy"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("good"), Bytes.toBytes("ball"));

        List<Put> list = new ArrayList();
        list.add(put);
        list.add(put1);

        table.put(list);
    }

    /**
     * 根据Rowkey进行查询
     */
    @Test
    public void Scan_by_Rowkey() throws Exception {
        String rowKey = "zhangsan";
        table = connection.getTable(TableName.valueOf("school"));
        Get get = new Get(rowKey.getBytes());

        Result result = table.get(get);
        System.out.println("列族\t列名\t内容");
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +  //Rowkey
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" + //CF
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +//qualifier
                            Bytes.toString(CellUtil.cloneValue(cell)) //value
            );
        }
    }

    /**
     * 根据Rowkey 和列祖和列进行查询内容
     */
    @Test
    public void Scan_by_Rowkey1() throws Exception {
        String rowKey = "zhangsan";
        table = connection.getTable(TableName.valueOf("school"));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("good"));

        Result result = table.get(get);
        System.out.println("列族rowKey = \"zhangsan\"列族为info,列为age的值是：");
        String age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
        System.out.println(age);
    }

    /**
     * Scan 全表  在实际生产当中是不这样干的，内容上百万千万的，很浪费时间的
     * S
     * 一般添加过滤器Filter
     */
    @Test
    public void Scan_Table() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result result :
                rs) {
            PrintResult(result);
        }
    }

    /**
     * Scan 查询 添加个条件，Scan 对象参数是  Rowkey
     */
    @Test
    public void Scan_Table2() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        Scan scan = new Scan(Bytes.toBytes("wangwu"));

        ResultScanner rs = table.getScanner(scan);
        for (Result result :
                rs) {
            PrintResult(result);
        }
    }

    /**
     * 定义私有方法 用于打印表的信息
     */
    private void PrintResult(Result result) {
        for (Cell cell :
                result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +//Rowkey
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +//CF
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +//qualifier
                            Bytes.toString(CellUtil.cloneValue(cell)) //value
            );
        }
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }


    /**
     * 条件过滤  Filter
     * 查询后最名 为  wu的
     * Hbase 中数据是按照字典顺序进行排列的
     * 不会因为插入的时间不同而不同
     */
    @Test
    public void Filter_01() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        String reg = "^*wu";
        org.apache.hadoop.hbase.filter.Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        for (Result result :
                results) {
            PrintResult(result);
        }
    }

    /**
     * 前罪名
     */
    @Test
    public void Filter_0sss() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        String reg = "^*wu";
        org.apache.hadoop.hbase.filter.Filter filter = new PrefixFilter(Bytes.toBytes("li"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        for (Result result :
                results) {
            PrintResult(result);
        }
    }

    /**
     * 多条件过滤  FilterList
     * <p>
     * MUST_PASS_ALL 全都满足条件
     * <p>
     * MUST_PASS_ONE  满足一个就可以
     */
    @Test
    public void FilterList() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        String reg = "^*wu";
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        org.apache.hadoop.hbase.filter.Filter filter = new PrefixFilter(Bytes.toBytes("li"));
        org.apache.hadoop.hbase.filter.Filter filter2 = new PrefixFilter(Bytes.toBytes("zh"));
        filterList.addFilter(filter);
        filterList.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner results = table.getScanner(scan);
        for (Result result :
                results) {
            PrintResult(result);
        }
    }

    /**
     * 删除数据
     */
    @Test
    public void Delete_Data() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        Delete delete = new Delete(Bytes.toBytes("lisi"));
        table.delete(delete);
    }

    @Test
    public void Delete_Data_mutilDelete() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        Delete delete = new Delete(Bytes.toBytes("zhangsan"));
        //删除当前版本的内容
        // delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes("good"));
        //删除所有版本
        //delete.addColumns(Bytes.toBytes("info"),Bytes.toBytes("good"));

        /**
         * addColumn addColumns 是不一样的 ，记住了，
         *
         * addColumn 是删除单元格当前最近版本的 内容，
         *
         * addColumns 是删除单元格所有版本的内容
         * */
        //删除列族
        delete.addFamily(Bytes.toBytes("info"));

        table.delete(delete);
    }

    /**
     * 删除表
     */
    @Test
    public void Drop_table() throws Exception {
        table = connection.getTable(TableName.valueOf("school"));
        if (admin.tableExists(TableName.valueOf("school"))) {
            admin.disableTable(TableName.valueOf("school"));
            admin.deleteTable(TableName.valueOf("school"));
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }
    }


}
