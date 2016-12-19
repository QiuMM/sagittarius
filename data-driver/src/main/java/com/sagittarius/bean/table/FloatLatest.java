package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_float
 */

@Table(name = "latest_float",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class FloatLatest extends AbstractLatest {
    private float value;

    public FloatLatest(String host, String metric, long createdAt, long receivedAt, float value) {
        super(host, metric, createdAt, receivedAt);
        this.value = value;
    }

    public FloatLatest() {
    }

    @Column(name = "value")
    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
