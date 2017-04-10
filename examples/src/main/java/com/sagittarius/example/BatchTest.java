package com.sagittarius.example;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class BatchTest extends  Thread{
    private static final Logger logger = LoggerFactory.getLogger(BatchTest.class);

    private SagittariusWriter writer;
    private String host;
    private Random random;
    private int runTime;
    private long count;
    private double throughput;

    public double getThroughput() {
        return throughput;
    }

    public long getCount() {
        return count;
    }

    public BatchTest(Writer writer, String host, int runTime) {
        this.writer = (SagittariusWriter)writer;
        this.host = host;
        this.runTime = runTime;
        count = 0;
        throughput = 0;
        random = new Random();
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - start) < runTime * 60 * 60 * 1000) {
            for (int i = 0; i < 3000; ++i) {
                writer.insert(host, "APP", time, time, TimePartition.DAY, random.nextLong());
                ++time;
            }
            count += 3000;
        }
    }
}
