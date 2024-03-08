package org.example.sparkStreamingwithKafka;

import org.graalvm.polyglot.Context;

import java.util.Objects;

public class GraalContextFactory {

    private static Context context;

    private GraalContextFactory(){

    }

    public static Context getInstance() {
        if (Objects.isNull(context)) {
            context = org.graalvm.polyglot.Context.newBuilder("js").allowAllAccess(true)
                    .build();
        }
        return context;
    }

}
