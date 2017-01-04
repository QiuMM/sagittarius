package com.sagittarius.bean.result;

/**
 * metric data point info, used to return data point result to users
 */
public abstract class AbstractPoint {
    private String metric;
    private long primaryTime;
    private long secondaryTime;

    public AbstractPoint(String metric, long primaryTime, long secondaryTime) {
        this.metric = metric;
        this.primaryTime = primaryTime;
        this.secondaryTime = secondaryTime;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getPrimaryTime() {
        return primaryTime;
    }

    public void setPrimaryTime(long primaryTime) {
        this.primaryTime = primaryTime;
    }

    public long getSecondaryTime() {
        return secondaryTime;
    }

    public void setSecondaryTime(long secondaryTime) {
        this.secondaryTime = secondaryTime;
    }
}
