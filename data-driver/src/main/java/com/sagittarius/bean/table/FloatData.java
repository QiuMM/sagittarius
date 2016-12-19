package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_float
 */

@Table(name = "data_float",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class FloatData extends AbstractData {
    private float value;

    public FloatData(String host, String metric, String date, long primaryTime, long secondaryTime, float value) {
        super(host, metric, date, primaryTime, secondaryTime);
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
