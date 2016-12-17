package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class BooleanPoint extends AbstractPoint {
    private boolean value;

    public BooleanPoint(String metric, long time, boolean value) {
        super(metric, time);
        this.value = value;
    }

    public boolean isValue() {

        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }
}
