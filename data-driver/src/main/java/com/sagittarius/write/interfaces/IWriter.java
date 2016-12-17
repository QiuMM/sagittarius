package com.sagittarius.write.interfaces;

import com.sagittarius.bean.batch.*;
import com.sagittarius.bean.table.HostMetric.ValueType;
import com.sagittarius.bean.table.HostMetric.DateInterval;

import java.util.List;
import java.util.Map;

/**
 * Created by qmm on 2016/12/15.
 */
public interface IWriter {
    void registerHostMetricInfo(String host, List<String> metrics, List<DateInterval> dateIntervals, List<ValueType> valueTypes);

    void registerHostTags(String host, Map<String, String> tags);

    void registerOwnerInfo(String user, List<String> hosts);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, int value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, long value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, float value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, double value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, boolean value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, String value);

    void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, float latitude, float longitude);

    void batchInsert(BatchIntData batchIntData);

    void batchInsert(BatchLongData batchLongData);

    void batchInsert(BatchFloatData batchFloatData);

    void batchInsert(BatchDoubleData batchDoubleData);

    void batchInsert(BatchBooleanData batchBooleanData);

    void batchInsert(BatchStringData batchStringData);

    void batchInsert(BatchGeoData batchGeoData);
}
