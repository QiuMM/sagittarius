package com.sagittarius.bean.result;

public class BooleanPoint extends AbstractPoint {
    private boolean value;

    public BooleanPoint(String metric, long primaryTime, long secondaryTime, boolean value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public boolean getValue() {
        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }
}
