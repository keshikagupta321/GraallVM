package org.example.sparkStreamingwithKafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

public class UdfTemplate<A, B> implements UDF2<A, B, Object>, Serializable {

    Context context;
    String functionName;
    String language;
    String functionCode;
    UdfRuntimeTest.DataTypeHolder schema;
    ObjectMapper mapper = new ObjectMapper();

    public UdfTemplate(String functionName, String language, String functionCode, String sampleJson) {
        this.functionName = functionName;
        this.language = language;
        this.functionCode = functionCode;
        this.schema = UdfRuntimeTest.DataTypeHolder.resolve(sampleJson);
    }

    public DataTypeHolder getSchema() {
        return schema;
    }



    @Override
    public Object call(A a, B b) throws Exception {
        context = GraalContextFactory.getInstance();
        System.out.println(context);
        context.eval(language, functionCode);
        Object finalParam1 = a;
        if(a instanceof GenericRowWithSchema) {
            GenericRowWithSchema param1 = (GenericRowWithSchema) a;
            finalParam1 = mapper.readValue(param1.json(), Map.class);
        }
        Value result = context.getBindings(language)
                .getMember(functionName).execute(finalParam1, b);

        String [] fieldNames = ((StructType)schema).fieldNames();

        Object[] fields = new Object[fieldNames.length];

        Map res = result.as(Map.class);

        populateFieldValuesAsPerSchema(fields, res, (StructType) schema);
        GenericRowWithSchema k = new GenericRowWithSchema(fields, (StructType) schema);

        return k;
    }

    private static void populateFieldValuesAsPerSchema(Object[] fields, Map res, StructType schema) {
        String [] fieldNames = schema.fieldNames();
        for(int i = 0; i< fields.length; i++){
            Object val = res.get(fieldNames[i]);
            if(val instanceof Map map){
                val = getGenericRowValue(schema, map, i);
            }else if(val instanceof List<?> list){
                StructType childType = ((StructType) schema.fields()[i].dataType());
                for (Object listItems : list){

                }
            } else if(val instanceof Long || val instanceof Integer){
                val = ((Number) val).doubleValue();
            }
            fields[i] = val;
        }
    }

    @NotNull
    private static Object getGenericRowValue(StructType schema, Map map, int i) {
        Object val;
        StructType childType = ((StructType) schema.fields()[i].dataType());
        Object[] childrenFields = new Object[childType.fieldNames().length];
        populateFieldValuesAsPerSchema(childrenFields, map, childType);
        val = new GenericRowWithSchema(childrenFields, childType);
        return val;
    }

}

