package com.apache.Hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class AllEmail extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new AllEmailEvaluator();
    }
    public static class AllEmailEvaluator extends GenericUDAFEvaluator{
         static  class MyBuffer implements AggregationBuffer{
             StringBuffer sb=new StringBuffer();
             void add(String s){
                 sb.append(s+",");
             }
             void clear(){
                 sb=new StringBuffer();
             }
         }
         private PrimitiveObjectInspector io;
         private String rs="";
         @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
             super.init(m, parameters);
             io= (PrimitiveObjectInspector) parameters[0];
             return ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MyBuffer();
        }

        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((MyBuffer)aggregationBuffer).clear();
        }

        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            Object p = io.getPrimitiveJavaObject(objects[0]);
            ((MyBuffer)aggregationBuffer).add(p.toString());
        }

        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((MyBuffer)aggregationBuffer).sb.toString();
        }

        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            ((MyBuffer)aggregationBuffer).add(o.toString());
        }

        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((MyBuffer)aggregationBuffer).sb.toString();
        }
    }

}
