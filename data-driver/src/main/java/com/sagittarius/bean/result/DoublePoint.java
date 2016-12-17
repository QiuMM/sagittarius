package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class DoublePoint extends AbstractPoint {
    private double value;

    public DoublePoint(String metric, long time, double value) {
        super(metric, time);
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
