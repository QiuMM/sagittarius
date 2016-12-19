package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by qmm on 2016/12/18.
 * class map to table latest_text
 */

@Table(name = "latest_text",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM")
public class StringLatest extends AbstractLatest {
    private String value;

    public StringLatest(String host, String metric, long primaryTime, long secondaryTime, String value) {
        super(host, metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public StringLatest() {
    }

    @Column(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
