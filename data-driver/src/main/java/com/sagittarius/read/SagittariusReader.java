package com.sagittarius.read;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.*;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Predicate;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;


public class SagittariusReader implements Reader {
    private MappingManager mappingManager;
    private Session session;
    private JavaSparkContext sparkContext;

    public SagittariusReader(Session session, MappingManager mappingManager, JavaSparkContext sparkContext) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.sparkContext = sparkContext;
    }

    private String getTableByType(ValueType valueType) {
        String table = null;
        switch (valueType) {
            case INT:
                table = "data_int";
                break;
            case LONG:
                table = "data_long";
                break;
            case FLOAT:
                table = "data_float";
                break;
            case DOUBLE:
                table = "data_double";
                break;
            case BOOLEAN:
                table = "data_boolean";
                break;
            case STRING:
                table = "data_text";
                break;
            case GEO:
                table = "data_geo";
                break;
        }
        return table;
    }

    private List<ResultSet> getPointResultSet(List<String> hosts, List<String> metrics, long time, ValueType valueType) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetrics(hostMetrics, time);
        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), entry.getKey(), time));
            ResultSet set = session.execute(statement);
            resultSets.add(set);
        }
        return resultSets;
    }

    @Override
    public Map<ValueType, Map<String, Set<String>>> getDataType(List<String> hosts, List<String> metrics) {
        Map<ValueType, Map<String, Set<String>>> dataTypeMap = new HashMap<>();
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);

        for (HostMetric hostMetric : hostMetrics) {
            ValueType type = hostMetric.getValueType();
            if (dataTypeMap.containsKey(type)) {
                Map<String, Set<String>> setMap = dataTypeMap.get(type);
                setMap.get("hosts").add(hostMetric.getHost());
                setMap.get("metrics").add(hostMetric.getMetric());
            } else {
                Map<String, Set<String>> setMap = new HashMap<>();
                Set<String> hostSet = new HashSet<>();
                Set<String> metricSet = new HashSet<>();
                hostSet.add(hostMetric.getHost());
                metricSet.add(hostMetric.getMetric());
                setMap.put("hosts", hostSet);
                setMap.put("metrics", metricSet);
                dataTypeMap.put(type, setMap);
            }
        }

        return dataTypeMap;
    }

    @Override
    public ValueType getDataType(String host, String metric) {
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRIC_QUERY_STATEMENT, host, metric));
        ResultSet rs = session.execute(statement);
        HostMetric hostMetric = mapper.map(rs).one();
        return hostMetric.getValueType();

    }

    @Override
    public Map<String, List<IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.INT);
        List<IntData> datas = new ArrayList<>();
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.LONG);
        List<LongData> datas = new ArrayList<>();
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.FLOAT);
        List<FloatData> datas = new ArrayList<>();
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.DOUBLE);
        List<DoubleData> datas = new ArrayList<>();
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.BOOLEAN);
        List<BooleanData> datas = new ArrayList<>();
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.STRING);
        List<StringData> datas = new ArrayList<>();
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.GEO);
        List<GeoData> datas = new ArrayList<>();
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private List<ResultSet> getPointResultSet(String host, String metric, long time, ValueType valueType, Shift shift) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRIC_QUERY_STATEMENT, host, metric));
        ResultSet rs = session.execute(statement);
        Result<HostMetric> hostMetric = mapper.map(rs);

        String timeSlice = TimeUtil.generateTimeSlice(time, hostMetric.one().getTimePartition());

        if (shift == Shift.NEAREST) {
            statement = new SimpleStatement(String.format(QueryStatement.POINT_BEFORE_SHIFT_QUERY_STATEMENT, table, host, metric, timeSlice, time));
            ResultSet setBefore = session.execute(statement);
            if (!setBefore.isExhausted())
                resultSets.add(setBefore);
            statement = new SimpleStatement(String.format(QueryStatement.POINT_AFTER_SHIFT_QUERY_STATEMENT, table, host, metric, timeSlice, time));
            ResultSet setAfter = session.execute(statement);
            if (!setAfter.isExhausted())
                resultSets.add(setAfter);
            return resultSets;
        }

        String queryStatement;
        switch (shift) {
            case BEFORE:
                queryStatement = QueryStatement.POINT_BEFORE_SHIFT_QUERY_STATEMENT;
                break;
            case AFTER:
                queryStatement = QueryStatement.POINT_AFTER_SHIFT_QUERY_STATEMENT;
                break;
            default:
                queryStatement = QueryStatement.POINT_QUERY_STATEMENT;
        }

        statement = new SimpleStatement(String.format(queryStatement, table, host, metric, timeSlice, time));
        ResultSet set = session.execute(statement);
        resultSets.add(set);
        return resultSets;
    }

    @Override
    public IntPoint getFuzzyIntPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.INT, shift);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        if (resultSets.size() == 1) {
            IntData data = mapper.map(resultSets.get(0)).one();
            return new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            IntData dataBefore = mapper.map(resultSets.get(0)).one();
            IntData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new IntPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new IntPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public LongPoint getFuzzyLongPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.LONG, shift);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        if (resultSets.size() == 1) {
            LongData data = mapper.map(resultSets.get(0)).one();
            return new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            LongData dataBefore = mapper.map(resultSets.get(0)).one();
            LongData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new LongPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new LongPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public FloatPoint getFuzzyFloatPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.FLOAT, shift);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        if (resultSets.size() == 1) {
            FloatData data = mapper.map(resultSets.get(0)).one();
            return new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            FloatData dataBefore = mapper.map(resultSets.get(0)).one();
            FloatData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new FloatPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new FloatPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public DoublePoint getFuzzyDoublePoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.DOUBLE, shift);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        if (resultSets.size() == 1) {
            DoubleData data = mapper.map(resultSets.get(0)).one();
            return new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            DoubleData dataBefore = mapper.map(resultSets.get(0)).one();
            DoubleData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new DoublePoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new DoublePoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public BooleanPoint getFuzzyBooleanPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.BOOLEAN, shift);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        if (resultSets.size() == 1) {
            BooleanData data = mapper.map(resultSets.get(0)).one();
            return new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            BooleanData dataBefore = mapper.map(resultSets.get(0)).one();
            BooleanData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new BooleanPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new BooleanPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public StringPoint getFuzzyStringPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.STRING, shift);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        if (resultSets.size() == 1) {
            StringData data = mapper.map(resultSets.get(0)).one();
            return new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
        } else {
            StringData dataBefore = mapper.map(resultSets.get(0)).one();
            StringData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new StringPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
            else
                return new StringPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
        }
    }

    @Override
    public GeoPoint getFuzzyGeoPoint(String hosts, String metrics, long time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.GEO, shift);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        if (resultSets.size() == 1) {
            GeoData data = mapper.map(resultSets.get(0)).one();
            return new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude());
        } else {
            GeoData dataBefore = mapper.map(resultSets.get(0)).one();
            GeoData dataAfter = mapper.map(resultSets.get(1)).one();
            if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                return new GeoPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getLatitude(), dataAfter.getLongitude());
            else
                return new GeoPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getLatitude(), dataBefore.getLongitude());
        }
    }

    private Result<Latest> getLatestResult(List<String> hosts, List<String> metrics) {
        String table = "latest";
        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_TIMESLICE_QUERY_STATEMENT, table, ReadHelper.generateInStatement(hosts), ReadHelper.generateInStatement(metrics)));
        ResultSet rs = session.execute(statement);
        Mapper<Latest> mapper = mappingManager.mapper(Latest.class);
        return mapper.map(rs);

    }

    private ResultSet getPointResultSet(String host, String metric, String timeSlice, ValueType valueType) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();
        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_POINT_QUERY_STATEMENT, table, host, metric, timeSlice));
        ResultSet set = session.execute(statement);
        return set;
    }

    @Override
    public Map<String, List<IntPoint>> getIntLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<IntData> mapperInt = mappingManager.mapper(IntData.class);
        Map<String, List<IntPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.INT);
            IntData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<LongData> mapperInt = mappingManager.mapper(LongData.class);
        Map<String, List<LongPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.LONG);
            LongData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<FloatData> mapperInt = mappingManager.mapper(FloatData.class);
        Map<String, List<FloatPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.FLOAT);
            FloatData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<DoubleData> mapperInt = mappingManager.mapper(DoubleData.class);
        Map<String, List<DoublePoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.DOUBLE);
            DoubleData data = mapperInt.map(rs).one();

            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<BooleanData> mapperInt = mappingManager.mapper(BooleanData.class);
        Map<String, List<BooleanPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.BOOLEAN);
            BooleanData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<StringData> mapperInt = mappingManager.mapper(StringData.class);
        Map<String, List<StringPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.STRING);
            StringData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<GeoData> mapperInt = mappingManager.mapper(GeoData.class);
        Map<String, List<GeoPoint>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.GEO);
            GeoData data = mapperInt.map(rs).one();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private List<String> getRangeQueryString(List<String> hosts, List<String> metrics, long startTime, long endTime, ValueType valueType) {
        String table = getTableByType(valueType);
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetrics(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();
        long startTimeSecond = startTime / 1000;
        long endTimeSecond = endTime / 1000;


        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = ReadHelper.generateInStatement(entry.getValue().get("hosts"));
            String metricsString = ReadHelper.generateInStatement(entry.getValue().get("metrics"));
            TimePartition timePartition;
            if (startTimeSlice.contains("D")) {
                timePartition = TimePartition.DAY;
            } else if (startTimeSlice.contains("W")) {
                timePartition = TimePartition.WEEK;
            } else if (startTimeSlice.contains("M")) {
                timePartition = TimePartition.MONTH;
            } else {
                timePartition = TimePartition.YEAR;
            }

            String endTimeSlice = TimeUtil.generateTimeSlice(endTime, timePartition);
            if (startTimeSlice.equals(endTimeSlice)) {
                String query = String.format(QueryStatement.IN_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, startTimeSlice, startTime, endTime);
                querys.add(query);
                continue;
            }

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, ZoneOffset.UTC);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, ZoneOffset.UTC);
            List<LocalDateTime> totalDates = new ArrayList<>();
            while (!start.isAfter(end)) {
                totalDates.add(start);
                switch (timePartition) {
                    case DAY:
                        start = start.plusDays(1);
                        break;
                    case WEEK:
                        start = start.plusWeeks(1);
                        break;
                    case MONTH:
                        start = start.plusMonths(1);
                        break;
                    case YEAR:
                        start = start.plusYears(1);
                        break;
                }
            }
            String startQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, startTimeSlice, ">=", startTime);
            querys.add(startQuery);
            for (int i = 1; i < totalDates.size() - 1; ++i) {
                //the last datetime may be in the same timepartition with the end datetime, so it should be processed separately.
                String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(ZoneOffset.UTC) * 1000, timePartition));
                querys.add(query);
            }
            LocalDateTime last = totalDates.get(totalDates.size() - 1);
            boolean ifRepeat = true;
            switch (timePartition) {
                case DAY:
                    if (last.getDayOfYear() != end.getDayOfYear())
                        ifRepeat = false;
                    break;
                case WEEK:
                    if (last.getDayOfWeek().compareTo(end.getDayOfWeek()) <= 0)
                        ifRepeat = false;
                    break;
                case MONTH:
                    if (last.getMonthValue() != end.getMonthValue())
                        ifRepeat = false;
                    break;
                case YEAR:
                    if (last.getYear() != end.getYear())
                        ifRepeat = false;
                    break;
            }
            if (!ifRepeat) {
                String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(ZoneOffset.UTC) * 1000, timePartition));
                querys.add(query);
            }
            String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, endTimeSlice, "<=", endTime);
            querys.add(endQuery);

        }

        return querys;
    }

    @Override
    public Map<String, List<IntPoint>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        if (endTime < startTime)
            return null;

        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.INT);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        List<IntData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.LONG);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        List<LongData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.FLOAT);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        List<FloatData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.DOUBLE);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        List<DoubleData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.BOOLEAN);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        List<BooleanData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.STRING);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        List<StringData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.GEO);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        List<GeoData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private Class getClassByType(ValueType valueType) {
        Class klass = null;
        switch (valueType) {
            case INT:
                klass = IntData.class;
                break;
            case LONG:
                klass = LongData.class;
                break;
            case FLOAT:
                klass = FloatData.class;
                break;
            case DOUBLE:
                klass = DoubleData.class;
                break;
            case BOOLEAN:
                klass = BooleanData.class;
                break;
            case STRING:
                klass = StringData.class;
                break;
            case GEO:
                klass = GeoData.class;
                break;
        }
        return klass;
    }

    private List<String> getRangeQueryPredicates(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        //spark driver query metadata
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetrics(hostMetrics, startTime);

        List<String> predicates = new ArrayList<>();
        long startTimeSecond = startTime / 1000;
        long endTimeSecond = endTime / 1000;

        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = ReadHelper.generateInStatement(entry.getValue().get("hosts"));
            String metricsString = ReadHelper.generateInStatement(entry.getValue().get("metrics"));
            TimePartition timePartition;
            if (startTimeSlice.contains("D")) {
                timePartition = TimePartition.DAY;
            } else if (startTimeSlice.contains("W")) {
                timePartition = TimePartition.WEEK;
            } else if (startTimeSlice.contains("M")) {
                timePartition = TimePartition.MONTH;
            } else {
                timePartition = TimePartition.YEAR;
            }

            String endTimeSlice = TimeUtil.generateTimeSlice(endTime, timePartition);
            if (startTimeSlice.equals(endTimeSlice)) {
                String predicate = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT, hostsString, metricsString, startTimeSlice, startTime, endTime);
                predicates.add(predicate);
                continue;
            }

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, ZoneOffset.UTC);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, ZoneOffset.UTC);
            List<LocalDateTime> totalDates = new ArrayList<>();
            while (!start.isAfter(end)) {
                totalDates.add(start);
                switch (timePartition) {
                    case DAY:
                        start = start.plusDays(1);
                        break;
                    case WEEK:
                        start = start.plusWeeks(1);
                        break;
                    case MONTH:
                        start = start.plusMonths(1);
                        break;
                    case YEAR:
                        start = start.plusYears(1);
                        break;
                }
            }

            String startPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT, hostsString, metricsString, startTimeSlice, ">=", startTime);
            predicates.add(startPredicate);

            for (int i = 1; i < totalDates.size() - 1; ++i) {
                //the last datetime may be in the same timepartition with the end datetime, so it should be processed separately.
                String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(ZoneOffset.UTC) * 1000, timePartition));
                predicates.add(predicate);
            }
            LocalDateTime last = totalDates.get(totalDates.size() - 1);
            boolean ifRepeat = true;
            switch (timePartition) {
                case DAY:
                    if (last.getDayOfYear() != end.getDayOfYear())
                        ifRepeat = false;
                    break;
                case WEEK:
                    if (last.getDayOfWeek().compareTo(end.getDayOfWeek()) <= 0)
                        ifRepeat = false;
                    break;
                case MONTH:
                    if (last.getMonthValue() != end.getMonthValue())
                        ifRepeat = false;
                    break;
                case YEAR:
                    if (last.getYear() != end.getYear())
                        ifRepeat = false;
                    break;
            }
            if (!ifRepeat) {
                String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(ZoneOffset.UTC) * 1000, timePartition));
                predicates.add(predicate);
            }

            String endPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT, hostsString, metricsString, endTimeSlice, "<=", endTime);
            predicates.add(endPredicate);
        }

        return predicates;
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();
        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            int value = row.getInt("value");
            IntPoint point = new IntPoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<IntPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<IntPoint>> map = new HashMap<>();
                List<IntPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;

    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            long value = row.getLong("value");
            LongPoint point = new LongPoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<LongPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<LongPoint>> map = new HashMap<>();
                List<LongPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            float value = row.getFloat("value");
            FloatPoint point = new FloatPoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<FloatPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<FloatPoint>> map = new HashMap<>();
                List<FloatPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            double value = row.getDouble("value");
            DoublePoint point = new DoublePoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<DoublePoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<DoublePoint>> map = new HashMap<>();
                List<DoublePoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            boolean value = row.getBoolean("value");
            BooleanPoint point = new BooleanPoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<BooleanPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<BooleanPoint>> map = new HashMap<>();
                List<BooleanPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);

        Map<String, String> tmap = new HashMap<>();
        tmap.put("keyspace", "sagittarius");
        tmap.put("table", "data_text");
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> resultRDD = sqlContext.read().format("org.apache.spark.sql.cassandra").options(tmap).load().select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            Dataset<Row> rdd = sqlContext.read().format("org.apache.spark.sql.cassandra").options(tmap).load().select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }
        Row[] rows = resultRDD.collect();

        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        for (Row row : rows) {
            String host = row.getString(0);
            String metric = row.getString(1);
            long primaryTime = row.getLong(2);
            long secondaryTime = row.get(3) != null ? row.getLong(3) : -1;
            String value = row.getString(5);
            StringPoint point = new StringPoint(metric, primaryTime, secondaryTime, value);
            if (result.containsKey(host)) {
                Map<String, List<StringPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<StringPoint>> map = new HashMap<>();
                List<StringPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        /*JavaRDD<GeoData> rangeQueryRDD = getRangeQueryRDD(hosts, metrics, startTime, endTime, ValueType.GEO);
        JavaRDD<GeoData> resultRDD = rangeQueryRDD.filter(data -> filter.test(data));
        List<GeoData> datas = resultRDD.collect();

        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                Map<String, List<GeoPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                } else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    map.put(metric, points);
                }
            } else {
                Map<String, List<GeoPoint>> map = new HashMap<>();
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                map.put(metric, points);
                result.put(host, map);
            }
        }*/

        return null;
    }

    @Override
    public Map<String, Map<String, Double>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType){
            case MIN:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double)e.getInt("value"),1d)))
                        .reduceByKey((a,b) -> new Tuple2<>(a._1()+b._1(), a._2()+b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1()/e._2()._2()))
                        .collectAsMap();
                break;
            }
        }
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType){
            case MIN:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double)e.getLong("value"),1d)))
                        .reduceByKey((a,b) -> new Tuple2<>(a._1()+b._1(), a._2()+b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1()/e._2()._2()))
                        .collectAsMap();
                break;
            }
        }
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType){
            case MIN:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double)e.getFloat("value"),1d)))
                        .reduceByKey((a,b) -> new Tuple2<>(a._1()+b._1(), a._2()+b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1()/e._2()._2()))
                        .collectAsMap();
                break;
            }
        }
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType){
            case MIN:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double)e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG:{
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double)e.getDouble("value"),1d)))
                        .reduceByKey((a,b) -> new Tuple2<>(a._1()+b._1(), a._2()+b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1()/e._2()._2()))
                        .collectAsMap();
                break;
            }
        }
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + " and " + filter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = resultRDD
                .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();

        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);

        Map<String, String> tmap = new HashMap<>();
        tmap.put("keyspace", "sagittarius");
        tmap.put("table", "data_text");
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> resultRDD = sqlContext.read().format("org.apache.spark.sql.cassandra").options(tmap).load().select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
        for (int i = 1; i < predicates.size(); ++i) {
            Dataset<Row> xrdd = sqlContext.read().format("org.apache.spark.sql.cassandra").options(tmap).load().select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + " and " + filter);
            resultRDD = resultRDD.union(xrdd);
        }

        Map<HostMetricPair, Double> datas = resultRDD.toJavaRDD()
                .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString(0), e.getString(1)), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();

        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        /*JavaRDD<GeoData> rangeQueryRDD = getRangeQueryRDD(hosts, metrics, startTime, endTime, ValueType.GEO);
        JavaRDD<GeoData> resultRDD = rangeQueryRDD.filter(data -> filter.test(data));
        Map<HostMetricPair, Double> datas = resultRDD
                .mapToPair(e -> new Tuple2<HostMetricPair, Double>(new HostMetricPair(e.getHost(), e.getMetric()), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();

        Map<String, Map<String, Double>> result = new HashMap<>();
        for (Map.Entry<HostMetricPair, Double> data : datas.entrySet()) {
            String host = data.getKey().getHost();
            String metric = data.getKey().getMetric();
            if (result.containsKey(host)) {
                result.get(host).put(metric, data.getValue());
            } else {
                Map<String, Double> map = new HashMap<>();
                map.put(metric, data.getValue());
                result.put(host, map);
            }
        }*/

        return null;
    }
}