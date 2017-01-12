package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table latest
 */
@Table(name = "latest")
public class Latest {
    private String host;
    private String metric;
    private String timeSlice;

    public Latest() {
    }

    public Latest(String host, String metric, String timeSlice) {
        this.host = host;
        this.metric = metric;
        this.timeSlice = timeSlice;
    }

    @PartitionKey
    @Column(name = "host")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @ClusteringColumn
    @Column(name = "metric")
    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    @Column(name = "time_slice")
    public String getTimeSlice() {
        return timeSlice;
    }

    public void setTimeSlice(String timeSlice) {
        this.timeSlice = timeSlice;
    }
}
