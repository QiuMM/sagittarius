package com.sagittarius.write.internals;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;
import static com.datastax.driver.mapping.Mapper.Option.timestamp;

/**
 * A batch of records that is or will be sent.
 */
public class RecordBatch {
    final long createdMs;

    final MappingManager mappingManager;
    final BatchStatement batchStatement;
    final Map<HostMetricPair, Latest> latestData;

    public RecordBatch(long now, MappingManager mappingManager) {
        this.createdMs = now;

        this.mappingManager = mappingManager;
        this.batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        this.latestData = new HashMap<>();
    }

    public int getRecordCount() {
        return batchStatement.size();
    }

    public BatchStatement sendableStatement() {
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        for (Map.Entry<HostMetricPair, Latest> entry : latestData.entrySet()) {
            Statement statement = latestMapper.saveQuery(entry.getValue(), saveNullFields(false));
            batchStatement.add(statement);
        }
        return batchStatement;
    }

    private void updateLatest(Latest candidate) {
        HostMetricPair pair = new HostMetricPair(candidate.getHost(), candidate.getMetric());
        if (latestData.containsKey(pair)) {
            if (latestData.get(pair).getTimeSlice().compareTo(candidate.getTimeSlice()) < 0)
                latestData.put(pair, candidate);
        } else {
            latestData.put(pair, candidate);
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
        Statement statement = dataMapper.saveQuery(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
        Statement statement = dataMapper.saveQuery(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
        Statement statement = dataMapper.saveQuery(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Statement statement = dataMapper.saveQuery(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
        Statement statement = dataMapper.saveQuery(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
        Statement statement = dataMapper.saveQuery(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
        Statement statement = dataMapper.saveQuery(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
        batchStatement.add(statement);
        updateLatest(new Latest(host, metric, timeSlice));
    }
}
