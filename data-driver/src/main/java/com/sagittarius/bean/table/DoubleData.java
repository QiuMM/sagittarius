package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_double
 */

@Table(name = "data_double",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class DoubleData extends AbstractData {
    private double value;

    public DoubleData(String host, String metric, String date, long createdAt, long receivedAt, double value) {
        super(host, metric, date, createdAt, receivedAt);
        this.value = value;
    }

    public DoubleData() {
    }

    @Column(name = "value")
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
