package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;

/**
 * Created by qmm on 2016/12/18.
 */
public class AbstractLatest {
    private String host;
    private String metric;
    private long createdAt;
    private long receivedAt;

    public AbstractLatest(String host, String metric, long createdAt, long receivedAt) {
        this.host = host;
        this.metric = metric;
        this.createdAt = createdAt;
        this.receivedAt = receivedAt;
    }

    public AbstractLatest() {
    }

    @PartitionKey(0)
    @Column(name = "host")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @PartitionKey(1)
    @Column(name = "metric")
    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    @Column(name = "created_at")
    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    @Column(name = "received_at")
    public long getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(long receivedAt) {
        this.receivedAt = receivedAt;
    }
}
