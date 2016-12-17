package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;

/**
 * Created by qmm on 2016/12/15.
 */
public abstract class AbstractData {
    private String host;
    private String metric;
    private String date;
    private long createdAt;
    private long receivedAt;

    public AbstractData() {
    }

    public AbstractData(String host, String metric, String date, long createdAt, long receivedAt) {
        this.host = host;
        this.metric = metric;
        this.date = date;
        this.createdAt = createdAt;
        this.receivedAt = receivedAt;
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

    @PartitionKey(2)
    @Column(name = "date")
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Column(name = "created_at")
    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    @ClusteringColumn
    @Column(name = "received_at")
    public long getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(long receivedAt) {
        this.receivedAt = receivedAt;
    }
}
