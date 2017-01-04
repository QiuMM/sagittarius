package com.sagittarius.bean.result;

public class IntPoint extends AbstractPoint {
    private int value;

    public IntPoint(String metric, long primaryTime, long secondaryTime, int value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
