package com.sagittarius.bean.result;

public class FloatPoint extends AbstractPoint {
    private float value;

    public FloatPoint(String metric, long primaryTime, long secondaryTime, float value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
