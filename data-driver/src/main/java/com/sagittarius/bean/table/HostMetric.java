package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table host_metric
 */

@Table(name = "host_metric",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class HostMetric {
    private String host;
    private String metric;
    private DateInterval dateInterval;
    private ValueType valueType;

    public HostMetric(String host, String metric, DateInterval dateInterval, ValueType valueType) {
        this.host = host;
        this.metric = metric;
        this.dateInterval = dateInterval;
        this.valueType = valueType;
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

    @Column(name = "date_interval")
    public DateInterval getDateInterval() {
        return dateInterval;
    }

    public void setDateInterval(DateInterval dateInterval) {
        this.dateInterval = dateInterval;
    }

    @Column(name = "value_type")
    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public static enum DateInterval {
        DAY, WEEK, MONTH, YEAR
    }

    public static enum ValueType {
        INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, GEO
    }
}
