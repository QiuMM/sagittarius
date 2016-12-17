package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class IntPoint extends AbstractPoint {
    private int value;

    public IntPoint(String metric, long time, int value) {
        super(metric, time);
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
