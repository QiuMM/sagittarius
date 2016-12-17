package com.sagittarius.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.batch.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.bean.table.HostMetric.DateInterval;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.interfaces.IWriter;

import java.util.List;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;

/**
 * Created by qmm on 2016/12/15.
 */
public class Writer implements IWriter{
    private Session session;
    private MappingManager mappingManager;

    public Writer(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
    }

    /**
     * one host corresponding to n metric, and one metric corresponding to one dateInterval and one valueTypes
     * @param host
     * @param metrics
     * @param dateIntervals
     * @param valueTypes
     */
    @Override
    public void registerHostMetricInfo(String host, List<String> metrics, List<HostMetric.DateInterval> dateIntervals, List<HostMetric.ValueType> valueTypes) {
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        for (int i = 0; i < metrics.size(); ++i) {
            mapper.save(new HostMetric(host, metrics.get(i), dateIntervals.get(0), valueTypes.get(0)), saveNullFields(false));
        }
    }

    @Override
    public void registerHostTags(String host, Map<String, String> tags) {
        Mapper<HostTags> mapper = mappingManager.mapper(HostTags.class);
        mapper.save(new HostTags(host, tags), saveNullFields(false));
    }

    @Override
    public void registerOwnerInfo(String user, List<String> hosts) {
        Mapper<Owner> mapper = mappingManager.mapper(Owner.class);
        for (String host : hosts) {
            mapper.save(new Owner(user, host), saveNullFields(false));
        }
    }

    @Override
    public void batchInsert(BatchIntData batchIntData) {
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (IntData data : batchIntData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchLongData batchLongData) {
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (LongData data : batchLongData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchFloatData batchFloatData) {
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (FloatData data : batchFloatData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchDoubleData batchDoubleData) {
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (DoubleData data : batchDoubleData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchBooleanData batchBooleanData) {
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (BooleanData data : batchBooleanData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchStringData batchStringData) {
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (StringData data : batchStringData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public void batchInsert(BatchGeoData batchGeoData) {
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        BatchStatement batchStatement = new BatchStatement();

        for (GeoData data : batchGeoData.getDatas()) {
            Statement statement = mapper.saveQuery(data, saveNullFields(false));
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    /**
     *
     * @param host
     * @param metric
     * @param createdAt can be null
     * @param receivedAt not null
     * @param dateInterval
     * @param value
     */
    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, int value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        mapper.save(new IntData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, long value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        mapper.save(new LongData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, float value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        mapper.save(new FloatData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, double value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        mapper.save(new DoubleData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, boolean value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        mapper.save(new BooleanData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, String value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        mapper.save(new StringData(host, metric, date, createdAt, receivedAt, value), saveNullFields(false));
    }

    @Override
    public void insert(String host, String metric, long createdAt, long receivedAt, DateInterval dateInterval, float latitude, float longitude) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        mapper.save(new GeoData(host, metric, date, createdAt, receivedAt, latitude, longitude), saveNullFields(false));
    }
}
