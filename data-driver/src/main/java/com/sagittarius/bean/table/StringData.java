package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_text
 */

@Table(name = "data_text",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class StringData extends AbstractData {
    private String value;

    public StringData(String host, String metric, String date, long createdAt, long receivedAt, String value) {
        super(host, metric, date, createdAt, receivedAt);
        this.value = value;
    }

    public StringData() {
    }

    @Column(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
