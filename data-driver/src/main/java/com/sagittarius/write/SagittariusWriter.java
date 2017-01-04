package com.sagittarius.write;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.bulk.*;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;
import static com.datastax.driver.mapping.Mapper.Option.timestamp;

public class SagittariusWriter implements Writer {
    private Session session;
    private MappingManager mappingManager;

    public SagittariusWriter(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
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

    private void insertAsync(List<Statement> statements, int threads) {
        List<ResultSetFuture> futures = new ArrayList<>();
        int count = 0;
        for (Statement statement : statements) {
            ResultSetFuture future = session.executeAsync(statement);
            futures.add(future);
            ++count;
            if(count % threads==0){
                futures.forEach(ResultSetFuture::getUninterruptibly);
                futures = new ArrayList<>();
            }
        }
    }

    @Override
    public void bulkInsert(BulkIntData bulkIntData, int threads) {
        Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        //each host_metric pair corresponding to it's latest data
        Map<HostMetricPair, IntData> latestData = new HashMap<>();

        //generate insert statements
        for (IntData data : bulkIntData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);
            //update latest data by comparing primaryTime
            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, IntData> entry : latestData.entrySet()) {
            IntData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        //execute insert statements by async
        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkLongData bulkLongData, int threads) {
        Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, LongData> latestData = new HashMap<>();

        for (LongData data : bulkLongData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, LongData> entry : latestData.entrySet()) {
            LongData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkFloatData bulkFloatData, int threads) {
        Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, FloatData> latestData = new HashMap<>();

        for (FloatData data : bulkFloatData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, FloatData> entry : latestData.entrySet()) {
            FloatData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkDoubleData bulkDoubleData, int threads) {
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, DoubleData> latestData = new HashMap<>();

        for (DoubleData data : bulkDoubleData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, DoubleData> entry : latestData.entrySet()) {
            DoubleData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkDoubleData bulkDoubleData) {
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        Map<HostMetricPair, DoubleData> latestData = new HashMap<>();

        for (DoubleData data : bulkDoubleData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, DoubleData> entry : latestData.entrySet()) {
            DoubleData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void bulkInsert(BulkBooleanData bulkBooleanData, int threads) {
        Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, BooleanData> latestData = new HashMap<>();

        for (BooleanData data : bulkBooleanData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, BooleanData> entry : latestData.entrySet()) {
            BooleanData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkStringData bulkStringData, int threads) {
        Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, StringData> latestData = new HashMap<>();

        for (StringData data : bulkStringData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, StringData> entry : latestData.entrySet()) {
            StringData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }

    @Override
    public void bulkInsert(BulkGeoData bulkGeoData, int threads) {
        Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
        Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
        List<Statement> statements = new ArrayList<>();
        Map<HostMetricPair, GeoData> latestData = new HashMap<>();

        for (GeoData data : bulkGeoData.getDatas()) {
            Statement statement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            statements.add(statement);

            HostMetricPair pair = new HostMetricPair(data.getHost(), data.getMetric());
            if (latestData.containsKey(pair)) {
                if (latestData.get(pair).getPrimaryTime() < data.getPrimaryTime())
                    latestData.put(pair, data);
            } else {
                latestData.put(pair, data);
            }
        }
        for (Map.Entry<HostMetricPair, GeoData> entry : latestData.entrySet()) {
            GeoData data = entry.getValue();
            Latest latest = new Latest(data.getHost(), data.getMetric(), data.getTimeSlice());
            Statement statement = latestMapper.saveQuery(latest, saveNullFields(false));
            statements.add(statement);
        }

        insertAsync(statements, threads);
    }
}
