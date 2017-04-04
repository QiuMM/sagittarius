package com.sagittarius.bean.common;

public class TypePartitionPair {
    private TimePartition timePartition;
    private ValueType valueType;

    public TypePartitionPair(TimePartition timePartition, ValueType valueType) {
        this.timePartition = timePartition;
        this.valueType = valueType;
    }

    public TimePartition getTimePartition() {
        return timePartition;
    }

    public void setTimePartition(TimePartition timePartition) {
        this.timePartition = timePartition;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }
}
