package org.example;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.example.sparkStreamingwithKafka.StreamKafka;
import org.example.sparkStreamingwithKafka.UdfRuntimeTest;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
     //   StreamKafka.addUserDefinedFunction();
        new UdfRuntimeTest().udf();




       // StreamKafka.checkAnotherUdf();
    }
}