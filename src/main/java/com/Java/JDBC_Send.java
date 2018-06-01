package com.Java;

import java.sql.*;
/*
* 测试jdbc驱动   连接
* */
public class JDBC_Send {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
     //   String URL="jdbc:mysql://192.168.3.20:3306/java_test?useUnicode=true&amp;characterEncoding=utf-8";
       String URL=  "jdbc:mysql://192.168.3.20:3306/java_test?characterEncoding=utf-8&useSSL=true";
        String USER="root";
        String PASSWORD="root";
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库链接
        Connection conn= DriverManager.getConnection(URL, USER, PASSWORD);
        //3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
        Statement st=conn.createStatement();
        ResultSet rs=st.executeQuery("select * from user");
        System.out.println("以下是 jdbc 查询输出内容 ");

        //4.处理数据库的返回结果(使用ResultSet类)
        while(rs.next()){
            System.out.println(rs.getString("name")+" "
                    +rs.getString("age"));
        }

        //关闭资源
        rs.close();
        st.close();
        conn.close();
    }
}

