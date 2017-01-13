package com.sagittarius.read;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;


public class SagittariusReader implements Reader {
    private MappingManager mappingManager;
    private Session session;

    public SagittariusReader(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
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
        Map<String, Map<String, Set<String>>> TimeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetrics(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();
        long startTimeSecond = startTime / 1000;
        long endTimeSecond = endTime / 1000;


        for (Map.Entry<String, Map<String, Set<String>>> entry : TimeSliceHostMetric.entrySet()) {
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
            if (startTimeSlice.equals(endTimeSlice)) { //同一天
                String query = String.format(QueryStatement.IN_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, startTimeSlice, startTime, endTime);
                querys.add(query);
                return querys;
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
}