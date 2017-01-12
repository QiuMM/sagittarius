package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_int
 */

@Table(name = "data_int")
public class IntData extends AbstractData {
    private int value;

    public IntData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, int value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
        this.value = value;
    }

    public IntData() {
    }

    @Column(name = "value")
    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
