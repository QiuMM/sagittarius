package com.sagittarius.bean.result;

public class LongPoint extends AbstractPoint {
    private long value;

    public LongPoint(String metric, long primaryTime, long secondaryTime, long value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
