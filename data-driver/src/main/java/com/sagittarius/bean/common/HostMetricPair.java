package com.sagittarius.bean.common;

public class HostMetricPair {
    private String host;
    private String metric;
    private int hashCode;

    public HostMetricPair(String host, String metric) {
        this.host = host;
        this.metric = metric;
        this.hashCode = generateHash();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof HostMetricPair) {
            HostMetricPair anotherPair = (HostMetricPair) obj;
            return host.equals(anotherPair.host) && metric.equals(anotherPair.metric);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int generateHash() {
        int result = 17;
        result = result * 31 + host.hashCode();
        result = result * 31 + metric.hashCode();
        return result;
    }
}
