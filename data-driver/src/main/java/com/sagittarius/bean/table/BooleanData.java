package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_boolean
 */

@Table(name = "data_boolean",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class BooleanData extends AbstractData {
    private boolean value;

    public BooleanData(String host, String metric, String date, long createdAt, long receivedAt, boolean value) {
        super(host, metric, date, createdAt, receivedAt);
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
