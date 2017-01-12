package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_boolean
 */

@Table(name = "data_boolean")
public class BooleanData extends AbstractData {
    private boolean value;

    public BooleanData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, boolean value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
        this.value = value;
    }

    public BooleanData() {
    }

    @Column(name = "value")
    public boolean getValue() {
        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }
}
