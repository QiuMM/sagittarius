package com.sagittarius.write;

import com.sagittarius.bean.batch.*;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.table.HostMetric.ValueType;
import com.sagittarius.bean.table.HostMetric.DateInterval;

import java.util.List;
import java.util.Map;

/**
 * Created by qmm on 2016/12/15.
 */
public interface Writer {
    void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas);

    void registerHostTags(String host, Map<String, String> tags);

    void registerOwnerInfo(String user, List<String> hosts);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, int value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, long value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, float value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, double value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, boolean value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, String value);

    void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, float latitude, float longitude);

    void batchInsert(BatchIntData batchIntData);

    void batchInsert(BatchLongData batchLongData);

    void batchInsert(BatchFloatData batchFloatData);

    void batchInsert(BatchDoubleData batchDoubleData);

    void batchInsert(BatchBooleanData batchBooleanData);

    void batchInsert(BatchStringData batchStringData);

    void batchInsert(BatchGeoData batchGeoData);
}
