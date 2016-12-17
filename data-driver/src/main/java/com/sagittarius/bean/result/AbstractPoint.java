package com.sagittarius.bean.result;

/**
 * Created by qmm on 2016/12/17.
 */
public class AbstractPoint {
    private String metric;
    private long time;

    public AbstractPoint(String metric, long time) {
        this.metric = metric;
        this.time = time;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
