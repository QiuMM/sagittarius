package com.sagittarius.bean.common;

/**
 * metric metadata info, ocntains the name(or id) of the metric and it's timePartition, valueType and description.
 */
public class MetricMetadata {
    private String metric;
    private TimePartition timePartition;
    private ValueType valueType;
    private String description;

    public MetricMetadata(String metric, TimePartition timePartition, ValueType valueType, String description) {
        this.metric = metric;
        this.timePartition = timePartition;
        this.valueType = valueType;
        this.description = description;
    }

    public String getMetric() {
        return metric;
    }

    public TimePartition getTimePartition() {
        return timePartition;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public String getDescription() {
        return description;
    }
}
