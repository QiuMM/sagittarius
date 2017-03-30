package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;

/**
 * basic fields for cassandra data tables
 */
public abstract class AbstractData {
    private String host;
    private String metric;
    private String timeSlice;
    private long primaryTime;
    //secondaryTime use boxed type so it can be set to null and won't be store in cassandra.
    //see com.datastax.driver.mapping.Mapper : saveNullFields
    private Long secondaryTime;

    public AbstractData() {
    }

    public AbstractData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime) {
        this.host = host;
        this.metric = metric;
        this.timeSlice = timeSlice;
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
    @Column(name = "time_slice")
    public String getTimeSlice() {
        return timeSlice;
    }

    public void setTimeSlice(String timeSlice) {
        this.timeSlice = timeSlice;
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
    public Long getSecondaryTime() {
        return secondaryTime;
    }

    public void setSecondaryTime(Long secondaryTime) {
        this.secondaryTime = secondaryTime;
    }

    public long secondaryTimeUnboxed(){
        if(secondaryTime==null)
            return -1;
        return secondaryTime;
    }
}
