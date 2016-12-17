package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class FloatPoint extends AbstractPoint {
    private float value;

    public FloatPoint(String metric, long time, float value) {
        super(metric, time);
        this.value = value;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
