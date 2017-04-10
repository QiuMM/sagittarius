package com.sagittarius.write.internals;

import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.TimePartition;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * This class acts as a queue that accumulates records to be sent to the server.
 */
public class RecordAccumulator {
    private final int batchSize;
    private final int lingerMs;
    private final Deque<RecordBatch> batches;

    private MappingManager mappingManager;

    public RecordAccumulator(int batchSize, int lingerMs, MappingManager mappingManager) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batches = new ArrayDeque<>();
        this.mappingManager = mappingManager;
    }

    public RecordBatch getReadyBatch(long nowMs) {
        synchronized (batches) {
            RecordBatch batch = batches.peekFirst();
            if (batch != null) {
                long waitedTimeMs = nowMs - batch.createdMs;
                boolean full = batches.size() > 1 || batch.getRecordCount() >= batchSize;
                boolean expired = waitedTimeMs >= lingerMs;
                if (full || expired) {
                    return batches.pollFirst();
                }
            }
            return null;
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
            }
        }
    }
}
