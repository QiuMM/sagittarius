package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_int
 */

@Table(name = "data_int",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class IntData extends AbstractData {
    private int value;

    public IntData(String host, String metric, String date, long primaryTime, long secondaryTime, int value) {
        super(host, metric, date, primaryTime, secondaryTime);
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
