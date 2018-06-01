package com.apache.Hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * 性能高 ， 难写
 * */
public class UDAF_test extends AbstractGenericUDAFResolver
{
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new UsingEmailUE();
    }
public static  class UsingEmailUE extends GenericUDAFEvaluator{
//接口一种标识  ， 序列化 ， 暂存对象
    static class MyAb implements AggregationBuffer{
        // 把所有的对象连接成一个
        StringBuilder sb ;
        void add(String value){
            sb.append(value);
            sb.append(",");
        };
        void clear(){
            sb = new StringBuilder();
        }
    }
    private PrimitiveObjectInspector io;  //
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
       // return super.init(m, parameters);  // 不能删除 ，
        super.init(m, parameters);
    io = (PrimitiveObjectInspector)parameters[0];
    return ObjectInspectorFactory.getReflectionObjectInspector(String.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }


    //聚合缓存
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new MyAb();
    }

    public void reset(AggregationBuffer agg) throws HiveException {
        ((MyAb)agg).clear();
    }

    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        Object value =   io.getPrimitiveJavaObject(parameters[0]) ;  // ?
        ((MyAb)agg).add(value.toString());//添加到缓存中
    }
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        return  ((MyAb)agg).sb.toString();//添加到缓存中;
    }
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        ((MyAb)agg).add(partial.toString());
    }
    public Object terminate(AggregationBuffer agg) throws HiveException {
        return   ((MyAb)agg).sb.toString();
    }
}
}
