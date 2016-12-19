package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_long
 */

@Table(name = "latest_long",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class LongLatest extends AbstractLatest {
    private long value;

    public LongLatest() {
    }

    public LongLatest(String host, String metric, long createdAt, long receivedAt, long value) {
        super(host, metric, createdAt, receivedAt);
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
