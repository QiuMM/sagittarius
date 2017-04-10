package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by mxw on 17-3-30.
 */
@Table(name = "data_aggregation")
public class AggregationData {
    private String host;
    private String metric;
    private long timeSlice;
    private double maxValue;
    private double minValue;
    private double countValue;
    private double sumValue;

    public AggregationData(){
    }

    public AggregationData(String host, String metric, long timeSlice, double maxValue, double minValue, double countValue, double sumValue){
        this.host = host;
        this.metric = metric;
        this.timeSlice =timeSlice;
        this.maxValue = maxValue;
        this.minValue = minValue;
        this.countValue = countValue;
        this.sumValue = sumValue;
    }

    @PartitionKey(0)
    @Column(name = "host")
    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }

    @PartitionKey(1)
    @Column(name = "metric")
    public String getMetric() {
        return metric;
    }
    public void setMetric(String metric) {
        this.metric = metric;
    }

    @ClusteringColumn
    @Column(name = "time_slice")
    public long getTimeSlice() {
        return timeSlice;
    }
    public void setTimeSlice(long timeSlice) {
        this.timeSlice = timeSlice;
    }

    @Column(name = "max_value")
    public double getMaxValue() {
        return maxValue;
    }
    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    @Column(name = "min_value")
    public double getMinValue() {
        return minValue;
    }
    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    @Column(name = "count_value")
    public double getCountValue(){return countValue;}
    public void setCountValue(double countValue){this.countValue = countValue;}

    @Column(name = "sum_value")
    public double getSumValue(){return sumValue;}
    public void setSumValue(double sumValue){this.sumValue = sumValue;}

}
