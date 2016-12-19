package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_double
 */

@Table(name = "latest_double",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class DoubleLatest extends AbstractLatest {
    private double value;

    public DoubleLatest(String host, String metric, long primaryTime, long secondaryTime, double value) {
        super(host, metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public DoubleLatest() {
    }

    @Column(name = "value")
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
