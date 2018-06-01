package com.apache.Hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UDF_test2 extends UDF{
    //反射原理
    //邮箱
    public Text evaluate (final Text value){
        return new Text(value.toString().substring(value.toString().indexOf("@")+1));
    }

//
    public Text evaluate(final Integer value){
    String temp="";
    Text text = new Text();
 if (value <=10 && value >0){
     temp ="小孩儿";
 }else  if (value >10 && value<=20){
     temp ="少年郎";
 }else  if (value >20 && value<=30){
     temp ="青年";
 }else  if (value >30 && value<=40){
     temp ="中年";
 }else  if (value >40 && value<=200){
     temp ="老年";
 }
 if(!"".equals(temp)&&temp!=null){
     text= new Text(temp.toString());
 }
    return text;
    }
    public static void main(String[] args) {

        UDF_test2 a = new UDF_test2();
       System.out.println( a.evaluate(new Text("4122222@qq.com")));
    }
}
