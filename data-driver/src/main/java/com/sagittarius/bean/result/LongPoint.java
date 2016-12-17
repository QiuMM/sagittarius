package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class LongPoint extends AbstractPoint {
    private long value;

    public LongPoint(String metric, long time, long value) {
        super(metric, time);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
