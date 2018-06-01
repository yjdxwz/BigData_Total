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

public class UsingEmail extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new UsingEmailUE();
    }
    public static class UsingEmailUE extends GenericUDAFEvaluator{
        static class MyAB implements AggregationBuffer{
            StringBuilder sb = new StringBuilder() ;
            void add(String value){
                sb.append(value);
                sb.append(",");
            }
            void clear(){
                sb=new StringBuilder();
            }

        }

        private PrimitiveObjectInspector io;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            io= (PrimitiveObjectInspector) parameters[0];
            return ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MyAB();
        }

        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((MyAB)aggregationBuffer).clear();
        }

        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
             Object value= io.getPrimitiveJavaObject(objects[0]);//?
            ((MyAB)aggregationBuffer).add(value.toString());
        }

        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((MyAB)aggregationBuffer).sb.toString();
        }

        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            ((MyAB)aggregationBuffer).add(o.toString());
        }

        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((MyAB)aggregationBuffer).sb.toString();
        }
    }
}
