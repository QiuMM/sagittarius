package com.sagittarius.example;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class WriteTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

    private Writer writer;
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

    public WriteTask(Writer writer, String host, int runTime) {
        this.writer = writer;
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
        long consumeTime = 0;
        while ((System.currentTimeMillis() - start) < runTime * 60 * 60 * 1000) {
            long startTime = System.currentTimeMillis();
            try {
                writer.insert(host, "APP", time, time, TimePartition.DAY, random.nextDouble() * 100);
                ++count;
            } catch (NoHostAvailableException | WriteTimeoutException e) {
                logger.info("Exception: ", e);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (QueryExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (com.sagittarius.exceptions.NoHostAvailableException e) {
                e.printStackTrace();
            }
            consumeTime += System.currentTimeMillis() - startTime;
            ++time;
            throughput = count / ((double) consumeTime / 1000);
        }
    }
}
