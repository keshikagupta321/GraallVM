package org.example.sparkStreamingwithKafka;

import org.graalvm.polyglot.Context;

import java.util.ArrayList;
import java.util.List;

public class GraalContextFactory {


   private static List<Context> pool;

    private static int poolSize = 5;

    public GraalContextFactory() {
        pool = new ArrayList<>();
        for (int i = 0; i < poolSize; i++) {
            pool.add(org.graalvm.polyglot.Context.newBuilder("js").allowAllAccess(true)
                    .build());
        }
    }

    public synchronized Context getInstance() throws InterruptedException {
        while (pool.isEmpty()) {
            wait();
        }
        return pool.remove(0);
    }

    public synchronized void returnToPool(Context context) {
        pool.add(context);
        notifyAll();
    }

}
