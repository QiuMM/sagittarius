package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_float
 */

@Table(name = "data_float",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class FloatData extends AbstractData {
    private float value;

    public FloatData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, float value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
        this.value = value;
    }

    public FloatData() {
    }

    @Column(name = "value")
    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
