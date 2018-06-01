package com.apache.Hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class UDF_test extends UDF
{

    //约定俗称 反射 机制
    public Text evaluate(final Text value) {
        if (value == null) { return null; }
        return new Text(value.toString().substring(value.toString().indexOf("@")+1));
    }


}
