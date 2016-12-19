package com.sagittarius.bean.common;

/**
 * Created by qmm on 2016/12/19.
 */
public class HostMetricPair {
    private String host;
    private String metric;

    public HostMetricPair(String host, String metric) {
        this.host = host;
        this.metric = metric;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof HostMetricPair) {
            HostMetricPair anotherPair = (HostMetricPair)obj;
            return host.equals(anotherPair.host) && metric.equals(anotherPair.metric);
        }
        return false;
    }
}
