package com.sagittarius.example;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class BatchWriteTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(BatchWriteTask.class);

    private SagittariusWriter writer;
    private String host;
    private Random random;
    private int batchSize;
    private int runTime;
    private long count;
    private double throughput;

    public double getThroughput() {
        return throughput;
    }
    public long getCount() {
        return count;
    }

    public BatchWriteTask(Writer writer, String host, int runTime, int batchSize) {
        this.writer = (SagittariusWriter)writer;
        this.host = host;
        this.runTime = runTime;
        this.batchSize = batchSize;
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
            SagittariusWriter.BulkData datas = writer.getBulkData();
            for (int i = 0; i < batchSize; ++i) {
                datas.addData(host, "APP", time, time, TimePartition.DAY, random.nextDouble() * 100);
                ++time;
            }
            long startTime = System.currentTimeMillis();
            try {
                writer.bulkInsert(datas);
                count += batchSize;
            } catch (NoHostAvailableException | WriteTimeoutException e) {
                logger.info("Exception: ", e);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            consumeTime += System.currentTimeMillis() - startTime;
            throughput = count / ((double) consumeTime / 1000);
        }
    }
}
