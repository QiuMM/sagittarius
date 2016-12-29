package com.sagittarius.write;

import com.sagittarius.bean.bulk.*;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;

import java.util.List;
import java.util.Map;

public interface Writer {
    /**
     * register host and it's metrics.
     * @param host the host name(or id)
     * @param metricMetadatas metrics info which belong to this host
     */
    void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas);

    /**
     * register tags to a host.
     * @param host the host name(or id)
     * @param tags a collection of tag_name:tag_value pairs
     */
    void registerHostTags(String host, Map<String, String> tags);

    /**
     * register user and it's hosts, state the ownership between users and hosts.
     * @param user the user name(or id)
     * @param hosts hosts belong to this user
     */
    void registerOwnerInfo(String user, List<String> hosts);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value);

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
    void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude);

    /**
     * bulk data insert for those metrics' value type is INT.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkIntData bulkIntData, int threads);

    /**
     * bulk data insert for those metrics' value type is LONG.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkLongData bulkLongData, int threads);

    /**
     * bulk data insert for those metrics' value type is FLOAT.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkFloatData bulkFloatData, int threads);

    /**
     * bulk data insert for those metrics' value type is DOUBLE.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkDoubleData bulkDoubleData, int threads);

    /**
     * bulk data insert for those metrics' value type is BOOLEAN.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkBooleanData bulkBooleanData, int threads);

    /**
     * bulk data insert for those metrics' value type is STRING.
     * tthis bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkStringData bulkStringData, int threads);

    /**
     * bulk data insert for those metrics' value type is GEO.
     * this bulk method implemented by asynchronous multi-threaded,
     * so you must specify how many threads to execute asynchronous
     * write. Be careful of the amount of threads you set for too
     * many threads may exhaust your resources.
     */
    void bulkInsert(BulkGeoData bulkGeoData, int threads);
}
