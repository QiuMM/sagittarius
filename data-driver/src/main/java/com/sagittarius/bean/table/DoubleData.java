package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_double
 */

@Table(name = "data_double")
public class DoubleData extends AbstractData {
    private double value;

    public DoubleData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, double value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
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
