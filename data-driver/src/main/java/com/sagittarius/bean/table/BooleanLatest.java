package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_boolean
 */

@Table(name = "latest_boolean",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class BooleanLatest extends AbstractLatest {
    private boolean value;

    public BooleanLatest(String host, String metric, long primaryTime, long secondaryTime, boolean value) {
        super(host, metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public BooleanLatest() {
    }

    @Column(name = "value")
    public boolean getValue() {
        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }
}
