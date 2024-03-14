package org.example.sparkStreamingwithKafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UdfTemplate<A, B> implements UDF2<A, B, Object>, Serializable {

    String functionName;
    String language;
    String functionCode;
    DataType schema;
    ObjectMapper mapper = new ObjectMapper();

    public UdfTemplate(String functionName, String language, String functionCode, String sample, DataType dataType) {
        this.functionName = functionName;
        this.language = language;
        this.functionCode = functionCode;
        this.schema = Objects.requireNonNullElseGet(dataType, () -> DataType.fromJson(sample.replaceAll("integer", "double").replaceAll("long", "double")));
    }

    public DataType getSchema() {
        return schema;
    }


    @Override
    public Object call(A a, B b) throws Exception {
        Long t1 = System.currentTimeMillis();
        GraalContextFactory contextFactory = new GraalContextFactory();
        Context context = contextFactory.getInstance();
        context.eval(language, functionCode);
        Object finalParam1 = a;
        Object finalParam2 = b;

        if (a instanceof GenericRowWithSchema) {
            finalParam1 = mapper.readValue(((GenericRowWithSchema) a).json(), Map.class);
            ;
        }
        if (b instanceof GenericRowWithSchema) {
            finalParam2 = mapper.readValue(((GenericRowWithSchema) b).json(), Map.class);
            ;
        }
        Value result = context.getBindings(language)
                .getMember(functionName).execute(finalParam1, finalParam2);

        if (schema instanceof StructType structType) {
            Object[] fieldNames = ((StructType) schema).fieldNames();
            Object[] fields = new Object[fieldNames.length];
            Map res = result.as(Map.class);
            populateFieldValuesAsPerSchema(fields, res, structType);
            GenericRowWithSchema k = new GenericRowWithSchema(fields, structType);
            System.out.println("Execution time: " + (System.currentTimeMillis() - t1));
            contextFactory.returnToPool(context);
            return k;
        } else if (schema instanceof IntegerType || schema instanceof LongType) {
            return result.asLong();
        } else if (schema instanceof StringType) {
            return result.asString();
        } else if (schema instanceof BooleanType) {
            return result.asBoolean();
        } else if (schema instanceof FloatType || schema instanceof DoubleType) {
            return result.asFloat();
        } else if (schema instanceof ArrayType arrayType) {
            return getChildObjectsArray(result.as(List.class), arrayType.elementType());
        } else {
            return result;
        }
    }


    private static void populateFieldValuesAsPerSchema(Object[] fields, Map<?, ?> res, StructType schema) {
        String[] fieldNames = schema.fieldNames();
        for (int i = 0; i < fields.length; i++) {
            Object val = res.get(fieldNames[i]);
            if (val instanceof Map<?, ?> map) {
                val = getGenericRowValue((StructType) schema.fields()[i].dataType(), map);
            } else if (val instanceof List<?> list) {
                ArrayType childType = ((ArrayType) schema.fields()[i].dataType());
                val = getChildObjectsArray(list, childType.elementType());
            } else if (val instanceof Long || val instanceof Integer) {
                val = ((Number) val).doubleValue();
            }
            fields[i] = val;
        }
    }

    @NotNull
    private static Object[] getChildObjectsArray(List<?> list, DataType childType) {
        Object[] objects = new Object[list.size()];
        int j = 0;
        for (Object listItem : list) {
            if (listItem instanceof Map<?, ?> map1) {
                objects[j++] = getGenericRowValue((StructType) childType, map1);
            } else if (listItem instanceof Number number) {
                objects[j++] = resolveNumericalType(number, childType);
            } else {
                objects[j++] = listItem;
            }
        }
        return objects;
    }

    private static Object resolveNumericalType(Number v, DataType childType) {
        if (childType instanceof DoubleType) {
            return v.doubleValue();
        } else if(childType instanceof IntegerType){
            return v.intValue();
        }
        else if(childType instanceof LongType){
            return v.longValue();
        }
        else if(childType instanceof StringType){
            return v.toString();
        }
        else
            throw new RuntimeException("Give Appropriate return type");
    }

    @NotNull
    private static GenericRowWithSchema getGenericRowValue(StructType childType, Map map) {
        Object[] childrenFields = new Object[childType.fieldNames().length];
        populateFieldValuesAsPerSchema(childrenFields, map, childType);
        return new GenericRowWithSchema(childrenFields, childType);
    }
}





