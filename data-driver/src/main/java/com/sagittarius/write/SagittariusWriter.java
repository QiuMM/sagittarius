package com.sagittarius.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.*;
import com.sagittarius.exceptions.*;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.internals.RecordAccumulator;
import com.sagittarius.write.internals.Sender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;
import static com.datastax.driver.mapping.Mapper.Option.timestamp;

public class SagittariusWriter implements Writer {
    private Session session;
    private MappingManager mappingManager;
    private RecordAccumulator accumulator;
    private Sender sender;
    private boolean autoBatch;

    public SagittariusWriter(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.autoBatch = false;
    }

    public SagittariusWriter(Session session, MappingManager mappingManager, int batchSize, int lingerMs) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.accumulator = new RecordAccumulator(batchSize, lingerMs, mappingManager);
        this.sender = new Sender(session, this.accumulator);
        this.autoBatch = true;
        Thread sendThread = new Thread(sender);
        sendThread.start();
    }

    public Data newData() {
        return new Data();
    }

    public class Data {
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

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
            Statement statement = dataMapper.saveQuery(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
            Statement statement = dataMapper.saveQuery(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
            Statement statement = dataMapper.saveQuery(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
            Statement statement = dataMapper.saveQuery(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
            Statement statement = dataMapper.saveQuery(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
            Statement statement = dataMapper.saveQuery(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
            Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
            Statement statement = dataMapper.saveQuery(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }
    }

    @Override
    public void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (MetricMetadata metricMetadata : metricMetadatas) {
                Statement statement = mapper.saveQuery(new HostMetric(host, metricMetadata.getMetric(), metricMetadata.getTimePartition(), metricMetadata.getValueType(), metricMetadata.getDescription()), saveNullFields(false));
                batchStatement.add(statement);
            }
            session.execute(batchStatement);
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void registerHostTags(String host, Map<String, String> tags) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<HostTags> mapper = mappingManager.mapper(HostTags.class);
            mapper.save(new HostTags(host, tags), saveNullFields(false));
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                //secondaryTime use boxed type so it can be set to null and won't be store in cassandra.
                //see com.datastax.driver.mapping.Mapper : saveNullFields
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), timestamp(primaryTime * 1000), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long timeSlice, double maxValue, double minValue, double countValue, double sumValue) {
        Mapper<AggregationData> dataMapper = mappingManager.mapper(AggregationData.class);
        dataMapper.save(new AggregationData(host, metric, timeSlice, maxValue, minValue, countValue, sumValue), timestamp(System.currentTimeMillis()), saveNullFields(false));
    }

    public void bulkInsert(Data data) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
            for (Map.Entry<HostMetricPair, Latest> entry : data.latestData.entrySet()) {
                Statement statement = latestMapper.saveQuery(entry.getValue(), saveNullFields(false));
                data.batchStatement.add(statement);
            }
            session.execute(data.batchStatement);
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }
}
