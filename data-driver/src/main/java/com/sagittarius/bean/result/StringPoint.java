package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class StringPoint extends AbstractPoint {
    private String value;

    public StringPoint(String metric, long time, String value) {
        super(metric, time);
        this.value = value;
    }

    public String getValue() {

        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
