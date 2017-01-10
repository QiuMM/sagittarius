package com.sagittarius.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;
import static com.datastax.driver.mapping.Mapper.Option.timestamp;

public class SagittariusWriter implements Writer {
    private Session session;
    private MappingManager mappingManager;
    private BulkData bulkData; //just for temporary use

    public SagittariusWriter(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
        bulkData = new BulkData();
    }

    public BulkData getBulkData() {
        return bulkData;
    }

    public class BulkData {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        Map<HostMetricPair, Latest> latestData = new HashMap<>();

        private void updateLatest(Latest candidate) {
            HostMetricPair pair = new HostMetricPair(candidate.getHost(), candidate.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getTimeSlice().compareTo(candidate.getTimeSlice()) < 0)
                    latestData.put(pair, candidate);
            } else {
                latestData.put(pair, candidate);
            }
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
            Statement statement = dataMapper.saveQuery(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
            Statement statement = dataMapper.saveQuery(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
            Statement statement = dataMapper.saveQuery(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
            Statement statement = dataMapper.saveQuery(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
            Statement statement = dataMapper.saveQuery(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
            Statement statement = dataMapper.saveQuery(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
            Statement statement = dataMapper.saveQuery(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }
    }

    @Override
    public void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas) {
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (MetricMetadata metricMetadata : metricMetadatas) {
            Statement statement = mapper.saveQuery(new HostMetric(host, metricMetadata.getMetric(), metricMetadata.getTimePartition(), metricMetadata.getValueType(), metricMetadata.getDescription()), saveNullFields(false));
            batchStatement.add(statement);
        }
        session.execute(batchStatement);
    }

    @Override
    public void registerHostTags(String host, Map<String, String> tags) {
        Mapper<HostTags> mapper = mappingManager.mapper(HostTags.class);
        mapper.save(new HostTags(host, tags), saveNullFields(false));
    }

    @Override
    public void registerOwnerInfo(String user, List<String> hosts) {
        Mapper<Owner> mapper = mappingManager.mapper(Owner.class);
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (String host : hosts) {
            Statement statement = mapper.saveQuery(new Owner(user, host), saveNullFields(false));
            batchStatement.add(statement);
        }
        session.execute(batchStatement);
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        //secondaryTime use boxed type so it can be set to null and won't be store in cassandra.
        //see com.datastax.driver.mapping.Mapper : saveNullFields
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
        Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        dataMapper.save(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
    }

    public void bulkInsert(BulkData bulkData) { //just for temporary use
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        for (Map.Entry<HostMetricPair, Latest> entry : bulkData.latestData.entrySet()) {
            Statement statement = latestMapper.saveQuery(entry.getValue(), saveNullFields(false));
            bulkData.batchStatement.add(statement);
        }
        session.execute(bulkData.batchStatement);
        bulkData.latestData.clear();
        bulkData.batchStatement.clear();
    }
}
