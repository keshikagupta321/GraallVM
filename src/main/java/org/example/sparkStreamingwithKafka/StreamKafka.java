package org.example.sparkStreamingwithKafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StreamKafka {
    static SparkSession spark = SparkSession
            .builder()
            .appName("JavaStructuredStreamingWithKafka")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", 1)
            .getOrCreate();

    static Dataset<Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "MyTopic")
            .load();

    public static void startStreaming() throws TimeoutException, StreamingQueryException {
        df.selectExpr("CAST(value AS STRING)").
                writeStream().format("console").outputMode("append").
                trigger(Trigger.ProcessingTime("2 seconds")).start().awaitTermination();
    }

//    public static void stopStreaming() throws TimeoutException {
//        df.selectExpr("CAST(value AS STRING)").
//                writeStream().format("console").outputMode("append").
//                trigger(Trigger.ProcessingTime("2 seconds")).start().stop();
//    }

    public static void addUserDefinedFunction() {
        UserDefinedFunction strLen = udf(
                (Integer x, Integer y) -> returnFromJS(x, y), DataTypes.IntegerType
        );

        spark.udf().register("strLen", strLen);
        spark.sql("SELECT strLen(4, 19)").show();
    }

    public static int returnFromJS(int x, int y) {
        try (Context context = Context.newBuilder("js")
                .allowAllAccess(true)
                .build()) {

            String jsCode = """
                        function sum(a , b){
                                let result = a + b;
                                return result;
                                }
                    """;

            var script = context.eval("js",jsCode);

            Value js = context.getBindings("js").getMember("sum").execute(x,y);
            return js.asInt();
        }
    }

    public static int returnFromNodeJs(int x, int y) {
        try (Context context = Context.newBuilder("js")
                .allowAllAccess(true)
                .build()) {

            String jsCode = """
                        function sum(a , b){
                                let result = a + b;
                                return result;
                                }
                    """;

            var script = context.eval("js",jsCode);

            Value js = context.getBindings("js").getMember("sum").execute(x,y);
            return js.asInt();
        }
    }

        public static int returnFromPython(int x, int y) {
        try (Context context = Context.newBuilder("python")
                .allowAllAccess(true)
                .build()) {
            Source src = Source.newBuilder("python", """
                            def multiply(a , b):
                                result = a * b;
                                return result;
                            multiply(a , b);
                            """,
                    "pythonComputation.py").buildLiteral();

            var bindings = context.getBindings("python");
            bindings.putMember("a", x);
            bindings.putMember("b", y);
            Value jsFunction = context.eval(src);
            int result = jsFunction.as(Number.class).intValue();
            java.lang.System.out.println("Python Result: " + result);
            return result;
        }
    }

    public static void checkAnotherUdf(){
        UserDefinedFunction random = udf(
                () -> Math.random(), DataTypes.DoubleType
        );
        spark.udf().register("random",random);
        spark.sql("SELECT random()").show();

    }


}
