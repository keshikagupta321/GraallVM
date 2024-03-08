package org.example.sparkStreamingwithKafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;


public class UdfRuntimeTest implements Serializable {

    static SparkSession spark = SparkSession
            .builder()
            .appName("RuntimeUdfTesting")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 1)
            .getOrCreate();

    public void udf() throws TimeoutException, StreamingQueryException {
        String jsCode = """
                    function sum(a,b){
                            let x = {"id":a.id,
                                     "name": a.name,
                                     "readings":{
                                        "temperature": a.readings.temperature,
                                        "pressure":{
                                            "P1":a.readings.pressure.P1,
                                            "P2":a.readings.pressure.P2
                                            }
                                     },
                                     "city" : b, 
                                     "some" : [ 
                                     {"id":a.id,
                                     "name": a.name,
                                     "readings":{
                                        "temperature": a.readings.temperature,
                                        "pressure":{
                                            "P1":a.readings.pressure.P1,
                                            "P2":a.readings.pressure.P2
                                            }
                                     },
                                     "city" : b
                                     },  43]
                                     };
                            return x;
                            }
                """;

        String sample = "{\"id\":123,\"name\":\"abc\",\"readings\": { \"temperature\":50.2, \"pressure\": {\"P1\":270.5,\"P2\":250 } },\"city\" : \"delhi\", \"some\"  : [{\"id\":123,\"name\":\"abc\",\"readings\": { \"temperature\":50.2, \"pressure\": {\"P1\":270.5,\"P2\":250 } },\"city\" : \"delhi\"}, 43]}";
        Dataset<String> dataFrame = spark.createDataset(Collections.singletonList(sample), Encoders.STRING());
        String schema = spark.read().json(dataFrame).schema().json();

        var x = new UdfTemplate("sum", "js", jsCode, schema);

        spark.sqlContext().udf().register("sum", x, x.DataTypeHolder.);

        //spark.sql("SELECT generic_udf('Hello', ' World')").show();
        spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .selectExpr("sum(struct(123 as id, 'abc' as name, struct(50.2 as temperature, struct(270.5 as P1, 250 as P2) as pressure) as readings), 'delhi') as data")
                .writeStream()
                .format("console")
                .start().awaitTermination();

    }

    public static class DataTypeHolder {
        private DataType dataType;

        private String name;

        private boolean struct;
        private List<DataTypeHolder> dataTypeHolders;

        public static DataTypeHolder resolve(String sampleJson){
            return new DataTypeHolder();
        }
    }
    public enum Types {
        STRING(DataTypes.StringType, String.class), INTEGER(DataTypes.IntegerType, Integer.class), LONG(DataTypes.LongType, Long.TYPE),
        DOUBLE(DataTypes.DoubleType, Double.TYPE), FLOAT(DataTypes.FloatType, Float.class), BOOLEAN(DataTypes.BooleanType, Boolean.class);

        public DataType getDataType() {
            return dataType;
        }

        private final Class<?> javaType;

        private final DataType dataType;

        Types(DataType dataType, Class<?> javaType) {
            this.javaType = javaType;
            this.dataType = dataType;
        }

        public Class<?> getJavaType() {
            return javaType;
        }
    }



}

