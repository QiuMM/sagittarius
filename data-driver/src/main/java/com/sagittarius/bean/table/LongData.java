package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * class map to cassandra table data_long
 */

@Table(name = "data_long",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class LongData extends AbstractData {
    private long value;

    public LongData() {
    }

    public LongData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, long value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
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
