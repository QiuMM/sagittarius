package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_int
 */
@Table(name = "latest_int",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class IntLatest extends AbstractLatest {
    private int value;

    public IntLatest(String host, String metric, long createdAt, long receivedAt, int value) {
        super(host, metric, createdAt, receivedAt);
        this.value = value;
    }

    public IntLatest() {
    }

    @Column(name = "value")
    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
