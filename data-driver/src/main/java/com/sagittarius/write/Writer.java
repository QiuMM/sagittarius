package com.sagittarius.write;

import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;

import java.util.List;
import java.util.Map;

public interface Writer {
    /**
     * register host and it's metrics(metric metadata).you can register metrics to a host at any time when you need to,
     * new metrics will be add to this host, duplicate metrics(metric metadata with the same metric field) will overwrite previous.
     * @param host the host name(or id)
     * @param metricMetadatas metrics info which belong to this host
     */
    void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * register tags to a host. you can register tags to a host at any time when you need to,
     * new tags will be added to this host, duplicate tags will overwrite previous.
     * @param host the host name(or id)
     * @param tags a collection of tag_name:tag_value pairs
     */
    void registerHostTags(String host, Map<String, String> tags) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is INT.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is LONG.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is FLOAT.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is DOUBLE.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is BOOLEAN.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is STRING.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param value the metric value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * insert a metric data point info whose metric value type is GEO.
     * for geo data there are fields: latitude and longitude.
     * there are two time parameters: primaryTime and secondaryTime.
     * primaryTime is more important than secondaryTime as their names imply.
     * the concern is there may be two time for a data point, one can be the
     * create time and another can be the receive time. You should set a time
     * which you think most important as the primaryTime. Besides, primaryTime
     * must have a meaningful value while secondaryTime can be null by setting
     * it to -1.
     * @param host the host name(or id)
     * @param metric the metric name(or id)
     * @param primaryTime timestamp in millisecond, must have a meaningful value
     * @param secondaryTime timestamp in millisecond, -1 means null
     * @param timePartition time partition
     * @param latitude latitude value
     * @param longitude longitude value
     */
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    void insert(String host, String metric, long timeSlice, double maxValue, double minValue, double countValue, double sumValue);
}
