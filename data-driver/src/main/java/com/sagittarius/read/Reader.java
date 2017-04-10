package com.sagittarius.read;


import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.*;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.GeoData;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Reader {
    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get map of valueType.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of valueType, the key is valueType, the value is a map containing set of hosts and set of metrics which are of this valueType
     */
    Map<ValueType, Map<String, Set<String>>> getValueType(List<String> hosts, List<String> metrics);

    /**
     * given host and metric, get the corresponding valueType.
     *
     * @param host   host
     * @param metric metric
     * @return valueType corresponding to that specified metric
     */
    ValueType getValueType(String host, String metric);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of IntPoints at the query time, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of LongPoints at the query time, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of FloatPoints at the query time, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String, Map<String, FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of DoublePoints at the query time, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of BooleanPoints at the query time, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of StringPoints at the query time, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of GeoPoints at the query time, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long time);

    /**
     * given host and metric , get a IntPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a IntPoint
     */
    IntPoint getFuzzyIntPoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a LongPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a LongPoint
     */
    LongPoint getFuzzyLongPoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a FloatPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a FloatPoint
     */
    FloatPoint getFuzzyFloatPoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a DoublePoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a DoublePoint
     */
    DoublePoint getFuzzyDoublePoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a BooleanPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a BooleanPoint
     */
    BooleanPoint getFuzzyBooleanPoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a StringPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a StringPoint
     */
    StringPoint getFuzzyStringPoint(String host, String metric, long time, Shift shift);

    /**
     * given host and metric , get a GeoPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a GeoPoint
     */
    GeoPoint getFuzzyGeoPoint(String host, String metric, long time, Shift shift);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of IntPoints at the latest time, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, List<IntPoint>>> getIntLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of LongPoints at the latest time, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, List<LongPoint>>> getLongLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of FloatPoints at the latest time, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String, Map<String, List<FloatPoint>>> getFloatLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of DoublePoints at the latest time, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, List<DoublePoint>>> getDoubleLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of BooleanPoints at the latest time, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, List<BooleanPoint>>> getBooleanLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of StringPoints at the latest time, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, List<StringPoint>>> getStringLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of GeoPoints at the latest time, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, List<GeoPoint>>> getGeoLatest(List<String> hosts, List<String> metrics);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of IntPoints, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of LongPoints, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of FloatPoints, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String , Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of DoublePoints, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of BooleanPoints, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of StringPoints, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of GeoPoints, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime);

    Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter);

    Map<String, Map<String, Double>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    Map<String, Map<String, Double>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType);

    void exportIntCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportLongCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportFloatCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportDoubleCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportBooleanCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportStringCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;

    void exportGeoCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException;


}
