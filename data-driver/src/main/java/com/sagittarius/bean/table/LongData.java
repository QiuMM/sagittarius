package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/15.
 * class map to table data_long
 */

@Table(name = "data_long",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class LongData extends AbstractData {
    private long value;

    public LongData() {
    }

    public LongData(String host, String metric, String date, long primaryTime, long secondaryTime, long value) {
        super(host, metric, date, primaryTime, secondaryTime);
        this.value = value;
    }

    @Column(name = "value")
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
