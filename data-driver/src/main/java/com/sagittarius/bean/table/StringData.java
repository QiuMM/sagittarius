package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_text
 */

@Table(name = "data_text")
public class StringData extends AbstractData {
    private String value;

    public StringData() {
    }

    public StringData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, String value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
        this.value = value;
    }

    @Column(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
