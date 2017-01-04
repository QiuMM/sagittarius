package com.sagittarius.bean.result;

public class DoublePoint extends AbstractPoint {
    private double value;

    public DoublePoint(String metric, long primaryTime, long secondaryTime, double value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
