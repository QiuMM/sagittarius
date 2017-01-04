package com.sagittarius.bean.result;

public class StringPoint extends AbstractPoint {
    private String value;

    public StringPoint(String metric, long primaryTime, long secondaryTime, String value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
