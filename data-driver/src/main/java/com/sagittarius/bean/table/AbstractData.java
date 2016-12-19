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
    private long primaryTime;
    private long secondaryTime;

    public AbstractData() {
    }

    public AbstractData(String host, String metric, String date, long primaryTime, long secondaryTime) {
        this.host = host;
        this.metric = metric;
        this.date = date;
        this.primaryTime = primaryTime;
        this.secondaryTime = secondaryTime;
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

    @ClusteringColumn
    @Column(name = "primary_time")
    public long getPrimaryTime() {
        return primaryTime;
    }

    public void setPrimaryTime(long primaryTime) {
        this.primaryTime = primaryTime;
    }

    @Column(name = "secondary_time")
    public long getSecondaryTime() {
        return secondaryTime;
    }

    public void setSecondaryTime(long secondaryTime) {
        this.secondaryTime = secondaryTime;
    }
}
