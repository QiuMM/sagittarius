package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;

/**
 * class map to cassandra table host_metric
 */
@Table(name = "host_metric",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class HostMetric {
    private String host;
    private String metric;
    private TimePartition timePartition;
    private ValueType valueType;
    private String description;

    public HostMetric(String host, String metric, TimePartition timePartition, ValueType valueType, String description) {
        this.host = host;
        this.metric = metric;
        this.timePartition = timePartition;
        this.valueType = valueType;
        this.description = description;
    }

    public HostMetric() {
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

    @Column(name = "time_partition")
    public TimePartition getTimePartition() {
        return timePartition;
    }

    public void setTimePartition(TimePartition timePartition) {
        this.timePartition = timePartition;
    }

    @Column(name = "value_type")
    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    @Column(name = "description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
