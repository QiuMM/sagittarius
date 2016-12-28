package com.sagittarius.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.batch.*;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.table.*;
import com.sagittarius.bean.table.HostMetric.DateInterval;
import com.sagittarius.util.TimeUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;
import static com.datastax.driver.mapping.Mapper.Option.timestamp;

/**
 * Created by qmm on 2016/12/15.
 */
public class SagittariusWriter implements com.sagittarius.write.interfaces.Writer {
    private Session session;
    private MappingManager mappingManager;

    public SagittariusWriter(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
    }

    @Override
    public void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas) {
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        BatchStatement batchStatement = new BatchStatement();
        for (MetricMetadata metricMetadata : metricMetadatas) {
            Statement statement = mapper.saveQuery(new HostMetric(host, metricMetadata.getMetric(), metricMetadata.getDateInterval(), metricMetadata.getValueType()), saveNullFields(false));
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
        BatchStatement batchStatement = new BatchStatement();
        for (String host : hosts) {
            Statement statement = mapper.saveQuery(new Owner(user, host), saveNullFields(false));
            batchStatement.add(statement);
        }
        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchIntData batchIntData) {
        Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
        Mapper<IntLatest> latestMapper = mappingManager.mapper(IntLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, IntData> latestData = new HashMap<>();

        for (IntData data : batchIntData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            IntLatest latest = new IntLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchLongData batchLongData) {
        Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
        Mapper<LongLatest> latestMapper = mappingManager.mapper(LongLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, LongData> latestData = new HashMap<>();

        for (LongData data : batchLongData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            LongLatest latest = new LongLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchFloatData batchFloatData) {
        Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
        Mapper<FloatLatest> latestMapper = mappingManager.mapper(FloatLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, FloatData> latestData = new HashMap<>();

        for (FloatData data : batchFloatData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            FloatLatest latest = new FloatLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchDoubleData batchDoubleData) {
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Mapper<DoubleLatest> latestMapper = mappingManager.mapper(DoubleLatest.class);
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        Map<HostMetricPair, DoubleData> latestData = new HashMap<>();

        for (DoubleData data : batchDoubleData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            DoubleLatest latest = new DoubleLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchBooleanData batchBooleanData) {
        Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
        Mapper<BooleanLatest> latestMapper = mappingManager.mapper(BooleanLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, BooleanData> latestData = new HashMap<>();

        for (BooleanData data : batchBooleanData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            BooleanLatest latest = new BooleanLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchStringData batchStringData) {
        Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
        Mapper<StringLatest> latestMapper = mappingManager.mapper(StringLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, StringData> latestData = new HashMap<>();

        for (StringData data : batchStringData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            StringLatest latest = new StringLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchGeoData batchGeoData) {
        Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
        Mapper<GeoLatest> latestMapper = mappingManager.mapper(GeoLatest.class);
        BatchStatement batchStatement = new BatchStatement();
        Map<HostMetricPair, GeoData> latestData = new HashMap<>();

        for (GeoData data : batchGeoData.getDatas()) {
            Statement dataStatement = dataMapper.saveQuery(data, timestamp(data.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(dataStatement);

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
            GeoLatest latest = new GeoLatest(data.getHost(), data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getLatitude(), data.getLongitude());
            Statement latestStatement = latestMapper.saveQuery(latest, timestamp(latest.getPrimaryTime() * 1000), saveNullFields(false));
            batchStatement.add(latestStatement);
        }

        session.execute(batchStatement);
    }


    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, int value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
        Mapper<IntLatest> latestMapper = mappingManager.mapper(IntLatest.class);
        dataMapper.save(new IntData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new IntLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, long value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
        Mapper<LongLatest> latestMapper = mappingManager.mapper(LongLatest.class);
        dataMapper.save(new LongData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new LongLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, float value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
        Mapper<FloatLatest> latestMapper = mappingManager.mapper(FloatLatest.class);
        dataMapper.save(new FloatData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new FloatLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, double value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
        Mapper<DoubleLatest> latestMapper = mappingManager.mapper(DoubleLatest.class);
        dataMapper.save(new DoubleData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new DoubleLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, boolean value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
        Mapper<BooleanLatest> latestMapper = mappingManager.mapper(BooleanLatest.class);
        dataMapper.save(new BooleanData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new BooleanLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, String value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
        Mapper<StringLatest> latestMapper = mappingManager.mapper(StringLatest.class);
        dataMapper.save(new StringData(host, metric, date, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new StringLatest(host, metric, primaryTime, secondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, DateInterval dateInterval, float latitude, float longitude) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
        Mapper<GeoLatest> latestMapper = mappingManager.mapper(GeoLatest.class);
        dataMapper.save(new GeoData(host, metric, date, primaryTime, secondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
        latestMapper.save(new GeoLatest(host, metric, primaryTime, secondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
    }
}
