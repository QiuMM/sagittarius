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
import com.sagittarius.bean.common.TypePartitionPair;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.AggregationType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.cache.Cache;
import com.sagittarius.read.internals.QueryStatement;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.SagittariusWriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.SizeEstimator;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class SagittariusReader implements Reader {
    private Session session;
    private MappingManager mappingManager;
    private JavaSparkContext sparkContext;
    private Cache<HostMetricPair, TypePartitionPair> cache;

    public SagittariusReader(Session session, MappingManager mappingManager, JavaSparkContext sparkContext, Cache<HostMetricPair, TypePartitionPair> cache) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.sparkContext = sparkContext;
        this.cache = cache;
    }

    private Map<String, Map<String, Set<String>>> getTimeSlicePartedHostMetrics(List<HostMetric> hostMetrics, long time) {
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = new HashMap<>();

        for (HostMetric hostMetric : hostMetrics) {
            String timeSlice = TimeUtil.generateTimeSlice(time, hostMetric.getTimePartition());
            if (timeSliceHostMetric.containsKey(timeSlice)) {
                Map<String, Set<String>> setMap = timeSliceHostMetric.get(timeSlice);
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
                timeSliceHostMetric.put(timeSlice, setMap);
            }
        }

        return timeSliceHostMetric;
    }

    private List<HostMetric> getHostMetrics(List<String> hosts, List<String> metrics) {
        List<HostMetric> result = new ArrayList<>();
        List<String> queryHosts = new ArrayList<>(), queryMetrics = new ArrayList<>();
        //first visit cache, if do not exist in cache, then query cassandra
        for (String host : hosts) {
            for (String metric : metrics) {
                TypePartitionPair typePartition = cache.get(new HostMetricPair(host, metric));
                if (typePartition != null) {
                    result.add(new HostMetric(host, metric, typePartition.getTimePartition(), typePartition.getValueType(), null));
                } else {
                    queryHosts.add(host);
                    queryMetrics.add(metric);
                }
            }
        }
        //query cassandra
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRICS_QUERY_STATEMENT, generateInStatement(queryHosts), generateInStatement(queryMetrics)));
        ResultSet rs = session.execute(statement);
        List<HostMetric> hostMetrics = mapper.map(rs).all();
        //update cache
        for (HostMetric hostMetric : hostMetrics) {
            cache.put(new HostMetricPair(hostMetric.getHost(), hostMetric.getMetric()), new TypePartitionPair(hostMetric.getTimePartition(), hostMetric.getValueType()));
        }

        result.addAll(hostMetrics);
        return result;
    }

    private String generateInStatement(Collection<String> params) {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append("'").append(param).append("'").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
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
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, time);
        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, generateInStatement(entry.getValue().get("hosts")), generateInStatement(entry.getValue().get("metrics")), entry.getKey(), time));
            ResultSet set = session.execute(statement);
            resultSets.add(set);
        }
        return resultSets;
    }

    @Override
    public Map<ValueType, Map<String, Set<String>>> getValueType(List<String> hosts, List<String> metrics) {
        Map<ValueType, Map<String, Set<String>>> result = new HashMap<>();
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        for (HostMetric hostMetric : hostMetrics) {
            ValueType type = hostMetric.getValueType();
            if (result.containsKey(type)) {
                Map<String, Set<String>> setMap = result.get(type);
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
                result.put(type, setMap);
            }

        }

        return result;
    }

    @Override
    public ValueType getValueType(String host, String metric) {
        List<String> hosts = new ArrayList<>(), metrics = new ArrayList<>();
        hosts.add(host);
        metrics.add(metric);
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        return hostMetrics.get(0).getValueType();
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.INT);
        List<IntData> datas = new ArrayList<>();
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<IntPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.LONG);
        List<LongData> datas = new ArrayList<>();
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<LongPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.FLOAT);
        List<FloatData> datas = new ArrayList<>();
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<FloatPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoublePoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.DOUBLE);
        List<DoubleData> datas = new ArrayList<>();
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<DoublePoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.BOOLEAN);
        List<BooleanData> datas = new ArrayList<>();
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<BooleanPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.STRING);
        List<StringData> datas = new ArrayList<>();
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<StringPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoPoint(List<String> hosts, List<String> metrics, long time) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.GEO);
        List<GeoData> datas = new ArrayList<>();
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                }
                else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                Map<String, List<GeoPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    private List<ResultSet> getPointResultSet(String host, String metric, long time, ValueType valueType, Shift shift) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();

        List<String> hosts = new ArrayList<>(), metrics = new ArrayList<>();
        hosts.add(host);
        metrics.add(metric);
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        if (hostMetrics.size() == 0) {
            return resultSets;
        }

        Statement statement;
        String timeSlice = TimeUtil.generateTimeSlice(time, hostMetrics.get(0).getTimePartition());

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

        String queryStatement = null;
        switch (shift) {
            case BEFORE:
                queryStatement = QueryStatement.POINT_BEFORE_SHIFT_QUERY_STATEMENT;
                break;
            case AFTER:
                queryStatement = QueryStatement.POINT_AFTER_SHIFT_QUERY_STATEMENT;
                break;
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
        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_TIMESLICE_QUERY_STATEMENT, table, generateInStatement(hosts), generateInStatement(metrics)));
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
    public Map<String, Map<String, List<IntPoint>>> getIntLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<IntData> mapperInt = mappingManager.mapper(IntData.class);
        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.INT);
            IntData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<IntPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<LongData> mapperInt = mappingManager.mapper(LongData.class);
        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.LONG);
            LongData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<LongPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<FloatData> mapperInt = mappingManager.mapper(FloatData.class);
        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.FLOAT);
            FloatData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<FloatPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoubleLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<DoubleData> mapperInt = mappingManager.mapper(DoubleData.class);
        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.DOUBLE);
            DoubleData data = mapperInt.map(rs).one();

            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<DoublePoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<BooleanData> mapperInt = mappingManager.mapper(BooleanData.class);
        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.BOOLEAN);
            BooleanData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<BooleanPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<StringData> mapperInt = mappingManager.mapper(StringData.class);
        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.STRING);
            StringData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<StringPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoLatest(List<String> hosts, List<String> metrics) {
        Result<Latest> latests = getLatestResult(hosts, metrics);
        ResultSet rs;
        Mapper<GeoData> mapperInt = mappingManager.mapper(GeoData.class);
        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();

        for (Latest latest : latests) {
            String host = latest.getHost();
            String metric = latest.getMetric();
            String timeSlice = latest.getTimeSlice();
            rs = getPointResultSet(host, metric, timeSlice, ValueType.GEO);
            GeoData data = mapperInt.map(rs).one();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(),data.getLongitude()));
                }
                else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                Map<String, List<GeoPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    private List<String> getRangeQueryString(List<String> hosts, List<String> metrics, long startTime, long endTime, ValueType valueType) {
        String table = getTableByType(valueType);
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();
        long startTimeSecond = startTime / 1000;
        long endTimeSecond = endTime / 1000;

        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = generateInStatement(entry.getValue().get("hosts"));
            String metricsString = generateInStatement(entry.getValue().get("metrics"));
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

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, TimeUtil.zoneOffset);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, TimeUtil.zoneOffset);
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
                String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * 1000, timePartition));
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
                String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * 1000, timePartition));
                querys.add(query);
            }
            String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, endTimeSlice, "<=", endTime);
            querys.add(endQuery);

        }

        return querys;
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        if (endTime < startTime)
            return null;

        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.INT);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        List<IntData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<IntPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.LONG);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        List<LongData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<LongPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.FLOAT);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        List<FloatData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<FloatPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.DOUBLE);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        List<DoubleData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<DoublePoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.BOOLEAN);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        List<BooleanData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<BooleanPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.STRING);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        List<StringData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                }
                else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<StringPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.GEO);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        List<GeoData> datas = new ArrayList<>();
        for (String query : querys) {
            ResultSet rs = session.execute(query);
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if(result.containsKey(host)){
                if(result.get(host).containsKey(metric)){
                    result.get(host).get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                }
                else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.get(host).put(metric, points);
                }
            }
            else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                Map<String, List<GeoPoint>> resultList = new HashMap<>();
                resultList.put(metric, points);
                result.put(host, resultList);
            }
        }

        return result;
    }

    private List<String> getRangeQueryPredicates(List<String> hosts, List<String> metrics, long startTime, long endTime) {
        //spark driver query metadata
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, startTime);

        List<String> predicates = new ArrayList<>();
        long startTimeSecond = startTime / 1000;
        long endTimeSecond = endTime / 1000;

        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = generateInStatement(entry.getValue().get("hosts"));
            String metricsString = generateInStatement(entry.getValue().get("metrics"));
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

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, TimeUtil.zoneOffset);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, TimeUtil.zoneOffset);
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
                String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * 1000, timePartition));
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
                String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * 1000, timePartition));
                predicates.add(predicate);
            }

            String endPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT, hostsString, metricsString, endTimeSlice, "<=", endTime);
            predicates.add(endPredicate);
        }

        return predicates;
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter) {
        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();

        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }

        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();


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
        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();


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
        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

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
        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

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
        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

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
        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String regex = filter.split(" ")[2];
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0)).filter(x -> x.getString("value").matches(regex));
        for (int i = 1; i < predicates.size(); ++i) {
            JavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i)).filter(x -> x.getString("value").matches(regex));
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            String value = row.getString("value");
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
        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }
        List<CassandraRow> rows = resultRDD.collect();

        for (CassandraRow row : rows) {
            String host = row.getString("host");
            String metric = row.getString("metric");
            long primaryTime = row.getLong("primary_time");
            long secondaryTime = row.getLong("secondary_time") != null ? row.getLong("secondary_time") : -1;
            float latitude = row.getFloat("latitude");
            float longitude = row.getFloat("longitude");
            GeoPoint point = new GeoPoint(metric, primaryTime, secondaryTime, latitude, longitude);
            if (result.containsKey(host)) {
                Map<String, List<GeoPoint>> map = result.get(host);
                if (map.containsKey(metric)) {
                    map.get(metric).add(point);
                } else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(point);
                    map.put(metric, points);
                }
            } else {
                Map<String, List<GeoPoint>> map = new HashMap<>();
                List<GeoPoint> points = new ArrayList<>();
                points.add(point);
                map.put(metric, points);
                result.put(host, map);
            }
        }

        return result;
    }

    @Override
    public Map<String, Map<String, Double>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) {
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = new HashMap<>();
        switch (aggregationType) {
            case MIN: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getInt("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double) e.getInt("value"), 1d)))
                        .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1() / e._2()._2()))
                        .collectAsMap();
                break;
            }
        }

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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType) {
            case MIN: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getLong("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double) e.getLong("value"), 1d)))
                        .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1() / e._2()._2()))
                        .collectAsMap();
                break;
            }
        }

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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType) {
            case MIN: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getFloat("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double) e.getFloat("value"), 1d)))
                        .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1() / e._2()._2()))
                        .collectAsMap();
                break;
            }
        }

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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = null;
        switch (aggregationType) {
            case MIN: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e2 : e1)
                        .collectAsMap();
                break;
            }
            case MAX: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 > e2 ? e1 : e2)
                        .collectAsMap();
                break;
            }
            case SUM: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), (double) e.getDouble("value")))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case COUNT: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                        .reduceByKey((e1, e2) -> e1 + e2)
                        .collectAsMap();
                break;
            }
            case AVG: {
                datas = resultRDD
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), new Tuple2<Double, Double>((double) e.getDouble("value"), 1d)))
                        .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                        .mapToPair(e -> new Tuple2<>(new HostMetricPair(e._1().getHost(), e._1().getMetric()), e._2()._1() / e._2()._2()))
                        .collectAsMap();
                break;
            }
        }

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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = resultRDD
                .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();


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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String regex = filter.split(" ")[2];
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0)).filter(x -> x.getString("value").matches(regex));
        for (int i = 1; i < predicates.size(); ++i) {
            JavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i)).filter(x -> x.getString("value").matches(regex));
            resultRDD = resultRDD.union(rdd);
        }

        Map<HostMetricPair, Double> datas = resultRDD
                .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();


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
        Map<String, Map<String, Double>> result = new HashMap<>();
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime);
        if (predicates.size() == 0) {
            return result;
        }
        String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(0) + queryFilter);
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(i) + queryFilter);
            resultRDD = resultRDD.union(rdd);
        }

        //only count
        Map<HostMetricPair, Double> datas = resultRDD
                .mapToPair(e -> new Tuple2<>(new HostMetricPair(e.getString("host"), e.getString("metric")), 1d))
                .reduceByKey((e1, e2) -> e1 + e2)
                .collectAsMap();

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

    public void test() throws IOException {
        long time = System.currentTimeMillis();

        long start = LocalDateTime.of(2017, 2, 26, 0, 0).toEpochSecond(TimeUtil.zoneOffset) * 1000;
        long end = LocalDateTime.of(2017, 2, 27, 23, 59).toEpochSecond(TimeUtil.zoneOffset) * 1000;
        String filePath = "test.txt";

        List<String> hosts = new ArrayList<>();

        hosts.add("131258");

        exportFloatCSV(hosts, getMetrics(), start, end, 240, null, filePath);
        /*Map<String, String> map = new HashMap<>();
        map.put("keyspace", "sagittarius");
        map.put("table", "data_float");
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> dataset = sqlContext.read().format("org.apache.spark.sql.cassandra").options(map).load().filter("value >= 33 and value <= 34");

        //dataset.filter(dataset.apply("value").$greater(33));
        //dataset.apply("").
        //dataset = dataset.filter("value >= 33 and value <= 34");
        //dataset = dataset.selectExpr("host");
        dataset.explain();
        System.out.println(dataset.count());

        System.out.println("consume time :" + (System.currentTimeMillis() - time));*/
        /*long start = LocalDateTime.of(2017, 2, 26, 0, 0).toEpochSecond(TimeUtil.zoneOffset) * 1000;
        long end = LocalDateTime.of(2017, 2, 27, 23, 59).toEpochSecond(TimeUtil.zoneOffset) * 1000;
        String filePath = "test.txt";
        List<String> hosts = new ArrayList<>();
        List<String> metrics = getMetrics();
        String host = "128275";
        hosts.add("128275");
        List<String> predicates = getRangeQueryPredicates(hosts, metrics, start, end);
        //String queryFilter = (filter == null) ? "" : " and " + filter;
        JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0));
        for (int i = 1; i < predicates.size(); ++i) {
            CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i));
            resultRDD = resultRDD.union(rdd);
        }

        List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                .stream()
                .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                .entrySet()
                .stream()
                .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                .collect(Collectors.toList());


        String lineSeparator = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        sb.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            sb.append(metric).append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        sb.append(lineSeparator);


        for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
            List<CassandraRow> rows = entry.getValue();
            CassandraRow first = rows.get(0);
            String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
            String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
            sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

            Map<String, String> metricMap = new HashMap<>();
            for (CassandraRow row : rows) {
                metricMap.put(row.getString("metric"), row.getString("value"));
                //System.out.println(row.getString("host") + " " + row.getString("metric") + " " + row.getDateTime("primary_time") + " " + row.getString("value"));
            }
            for (String metric : metrics) {
                sb.append(metricMap.getOrDefault(metric, "")).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(lineSeparator);
        }

        FileWriter fw = new FileWriter(filePath, true);
        fw.write(sb.toString());
        fw.close();
        //JavaRDD<CassandraRow> rdd1 = rdd.filter(r -> r.getFloat("value") >= 33 && r.getFloat("value") <= 34);
        /*resultRDD
                .groupBy(r -> r.getLong("primary_time"))
                .sortByKey()
                .collect();

        for (Long primaryTime : map.keySet()) {
            Iterable<CassandraRow> iter = map.get(primaryTime);
            iter.forEach(r -> System.out.println(r.getLong("primary_time") + " " + r.getString("metric") + " " + r.getFloat("value")));
        }*/
        //resultRDD.repartition(1).saveAsTextFile("E:\\2018\\a.txt");

        //System.out.println(rdd.count());
        //CassandraRow r = rdd.collect().get(0);*/

        System.out.println("consume time :" + (System.currentTimeMillis() - time) + " ");
    }

    private List<String> getHosts() {
        List<String> hosts = new ArrayList<>();
        hosts.add("128275");
        hosts.add("128579");
        hosts.add("128932");
        hosts.add("128933");
        hosts.add("128934");
        hosts.add("128935");
        hosts.add("128938");
        hosts.add("128946");
        hosts.add("128949");
        hosts.add("128952");
        hosts.add("128956");
        hosts.add("128963");
        hosts.add("128964");
        hosts.add("128967");
        hosts.add("128970");
        hosts.add("128997");
        hosts.add("128998");
        hosts.add("129000");
        hosts.add("129002");
        hosts.add("129003");
        hosts.add("129007");
        hosts.add("129008");
        hosts.add("129011");
        hosts.add("129017");
        hosts.add("129019");
        hosts.add("129020");
        hosts.add("129028");
        hosts.add("130128");
        hosts.add("130276");
        hosts.add("130277");
        hosts.add("130278");
        hosts.add("130279");
        hosts.add("130280");
        hosts.add("130289");
        hosts.add("130290");
        hosts.add("130291");
        hosts.add("130727");
        hosts.add("130888");
        hosts.add("130889");
        hosts.add("130890");
        hosts.add("130891");
        hosts.add("130892");
        hosts.add("130893");
        hosts.add("130894");
        hosts.add("130895");
        hosts.add("130896");
        hosts.add("130945");
        hosts.add("130946");
        hosts.add("130965");
        hosts.add("130966");
        hosts.add("130967");
        hosts.add("131009");
        hosts.add("131010");
        hosts.add("131011");
        hosts.add("131015");
        hosts.add("131023");
        hosts.add("131024");
        hosts.add("131031");
        hosts.add("131053");
        hosts.add("131058");
        hosts.add("131062");
        hosts.add("131084");
        hosts.add("131093");
        hosts.add("131094");
        hosts.add("131097");
        hosts.add("131128");
        hosts.add("131134");
        hosts.add("131135");
        hosts.add("131148");
        hosts.add("131151");
        hosts.add("131257");
        hosts.add("131258");
        hosts.add("131275");
        hosts.add("131429");
        hosts.add("131495");
        hosts.add("131631");
        hosts.add("131725");
        hosts.add("131727");
        hosts.add("131737");
        hosts.add("131738");
        hosts.add("131739");
        hosts.add("131966");
        hosts.add("131967");
        hosts.add("131968");
        hosts.add("131970");
        hosts.add("131971");
        hosts.add("131972");
        hosts.add("131973");
        hosts.add("131974");
        hosts.add("131977");
        hosts.add("131978");
        hosts.add("131979");
        hosts.add("131980");
        hosts.add("131984");
        hosts.add("131985");
        hosts.add("131986");
        hosts.add("131987");
        hosts.add("131988");
        hosts.add("131989");
        hosts.add("131990");
        hosts.add("131991");
        hosts.add("131993");
        hosts.add("131995");
        hosts.add("131996");
        hosts.add("131997");
        hosts.add("131998");
        hosts.add("131999");
        hosts.add("132000");
        hosts.add("132644");
        hosts.add("132745");
        hosts.add("132957");
        hosts.add("132965");
        hosts.add("133018");
        hosts.add("133384");
        hosts.add("133472");
        hosts.add("133473");
        hosts.add("133501");
        hosts.add("133674");
        hosts.add("133675");
        hosts.add("133678");
        hosts.add("134063");
        hosts.add("134277");
        hosts.add("134286");
        hosts.add("134294");
        hosts.add("134316");
        hosts.add("134351");
        hosts.add("134367");
        hosts.add("134406");
        hosts.add("134692");
        hosts.add("134695");
        hosts.add("134706");
        hosts.add("134723");
        hosts.add("134738");
        hosts.add("135133");
        hosts.add("135161");
        hosts.add("135163");
        hosts.add("135173");
        hosts.add("135231");
        hosts.add("135233");
        hosts.add("135249");
        hosts.add("135260");
        hosts.add("135262");
        hosts.add("135470");
        hosts.add("135837");
        hosts.add("135851");
        hosts.add("135892");
        hosts.add("135893");
        hosts.add("135894");
        hosts.add("135895");
        hosts.add("135896");
        hosts.add("135897");
        hosts.add("135898");
        hosts.add("135899");
        hosts.add("135901");
        hosts.add("135902");
        hosts.add("135903");
        hosts.add("135905");
        hosts.add("135906");
        hosts.add("135907");
        hosts.add("135908");
        hosts.add("135921");
        hosts.add("135922");
        hosts.add("135923");
        hosts.add("135924");
        hosts.add("135931");
        hosts.add("136608");
        hosts.add("136657");
        hosts.add("136679");
        hosts.add("136680");
        hosts.add("136846");
        hosts.add("136849");
        hosts.add("138023");
        hosts.add("138685");
        hosts.add("138733");
        hosts.add("138734");
        hosts.add("138998");
        hosts.add("138999");
        hosts.add("139024");
        hosts.add("139025");
        hosts.add("139026");
        hosts.add("139027");
        hosts.add("139028");
        hosts.add("139029");
        hosts.add("139030");
        hosts.add("139033");
        hosts.add("139034");
        hosts.add("139035");
        hosts.add("139036");
        hosts.add("139037");
        hosts.add("139040");
        hosts.add("139041");
        hosts.add("139042");
        hosts.add("139043");
        hosts.add("139045");
        hosts.add("139047");
        hosts.add("139049");
        hosts.add("139050");
        hosts.add("139052");
        hosts.add("139053");
        hosts.add("139065");
        hosts.add("139225");
        hosts.add("139291");
        hosts.add("139292");
        hosts.add("139300");
        hosts.add("139302");
        hosts.add("139307");
        hosts.add("139308");
        hosts.add("139309");
        hosts.add("139310");
        hosts.add("139313");
        hosts.add("139317");
        hosts.add("139318");
        hosts.add("139319");
        hosts.add("139324");
        hosts.add("139334");
        hosts.add("139350");
        hosts.add("139354");
        hosts.add("139356");
        hosts.add("139357");
        hosts.add("139399");
        hosts.add("139400");
        hosts.add("139401");
        hosts.add("139402");
        hosts.add("139403");
        hosts.add("139405");
        hosts.add("139409");
        hosts.add("139420");
        hosts.add("139421");
        hosts.add("139428");
        hosts.add("139431");
        hosts.add("139436");
        hosts.add("139445");
        hosts.add("139446");
        hosts.add("139450");
        hosts.add("139466");
        hosts.add("139475");
        hosts.add("139478");
        hosts.add("139487");
        hosts.add("139503");
        hosts.add("139591");
        hosts.add("139592");
        hosts.add("139593");
        hosts.add("139594");
        hosts.add("139658");
        hosts.add("139660");
        hosts.add("139661");
        hosts.add("139662");
        hosts.add("139663");
        hosts.add("139664");
        hosts.add("139672");
        hosts.add("139673");
        hosts.add("139676");

        return hosts;
    }

    private List<String> getMetrics() {
        List<String> metrics = new ArrayList<>();
        metrics.add("加速踏板位置1");
        metrics.add("当前转速下发动机负载百分比");
        metrics.add("实际发动机扭矩百分比");
        metrics.add("发动机转速");
        metrics.add("高精度总里程(00)");
        metrics.add("总发动机怠速使用燃料");
        metrics.add("后处理1排气质量流率");
        metrics.add("总发动机操作时间");
        metrics.add("总发动机使用的燃油");
        metrics.add("发动机冷却液温度");
        metrics.add("基于车轮的车辆速度");
        metrics.add("发动机燃料消耗率");
        metrics.add("大气压力");
        metrics.add("发动机进气歧管1压力");
        metrics.add("发动机进气歧管1温度");
        metrics.add("发动机进气压力");
        metrics.add("车速");
        metrics.add("大气温度");
        metrics.add("发动机进气温度");
        metrics.add("高精度总里程(EE)");
        metrics.add("后处理1进气氮氧化物浓度");
        metrics.add("后处理1进气氧气浓度");
        return metrics;
    }

    @Override
    public void exportIntCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportLongCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportFloatCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportDoubleCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportBooleanCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportStringCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String regex = filter.split(" ")[2];
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0)).filter(x -> x.getString("value").matches(regex));
                for (int i = 1; i < predicates.size(); ++i) {
                    JavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i)).filter(x -> x.getString("value").matches(regex));
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("value"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    @Override
    public void exportGeoCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException {
        //generate time ranges
        List<Long> timePoints = new ArrayList<>();
        timePoints.add(startTime);
        long splitMillis = splitHours * 60 * 60 * 1000;
        while (startTime + splitMillis < endTime) {
            startTime = startTime + splitMillis;
            timePoints.add(startTime);
        }
        timePoints.add(endTime);

        String lineSeparator = System.getProperty("line.separator");
        FileWriter fw = new FileWriter(filePath, true);
        //construct file header and write to file
        StringBuilder head = new StringBuilder();
        head.append("ID号,").append("生成时间,").append("接收时间,");
        for (String metric : metrics) {
            head.append(metric).append(",");
        }
        head.delete(head.length() - 1, head.length());
        head.append(lineSeparator);
        fw.write(head.toString());
        fw.flush();

        String queryFilter = (filter == null) ? "" : " and " + filter;
        //construct data output and write to file
        for (String host : hosts) {
            List<String> single = new ArrayList<>();
            single.add(host);
            for (int j = 0; j < timePoints.size() - 1; ++j) {
                List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1));
                if (predicates.size() == 0) {
                    continue;
                }
                //query cassandra
                JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(0) + queryFilter);
                for (int i = 1; i < predicates.size(); ++i) {
                    CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(i) + queryFilter);
                    resultRDD = resultRDD.union(rdd);
                }

                List<Map.Entry<Long, List<CassandraRow>>> entryList = resultRDD.collect()
                        .stream()
                        .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
                        .entrySet()
                        .stream()
                        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
                    List<CassandraRow> rows = entry.getValue();
                    CassandraRow first = rows.get(0);
                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");

                    Map<String, String> metricMap = new HashMap<>();
                    for (CassandraRow row : rows) {
                        metricMap.put(row.getString("metric"), row.getString("latitude") + "#" + row.getString("longitude"));
                    }
                    for (String metric : metrics) {
                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    sb.append(lineSeparator);
                }
                //write to file
                fw.write(sb.toString());
                fw.flush();
            }
        }

        fw.close();
    }

    private List<AggregationData> getAggResult2(String filter) {
        SimpleStatement statement = new SimpleStatement("select * from data_aggregation where " + filter);
        ResultSet resultSet = session.execute(statement);
        Mapper<AggregationData> mapper = mappingManager.mapper(AggregationData.class);
        List<AggregationData> result = mapper.map(resultSet).all();
        return result;
    }

    private String getAggregationDataPredicate2(String host, String metric, long startHour, long endHour, String filter, AggregationType aggregationType){
        // TODO: 17-4-6 what if string data and geo data?
        String predicate = null;
        switch (aggregationType){
            case MAX:{
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice <= " + endHour + queryFilter);
                break;
            }
            case MIN:{
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice <= " + endHour + queryFilter);
                break;
            }
            case COUNT:{
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value") + " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice <= " + endHour + queryFilter);
                break;
            }
            case SUM:{
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value") + " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice <= " + endHour + queryFilter);
            }
        }
        return predicate;
    }

    Map<String, Map<String, Double>> getNumericRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType, ValueType valueType){
        Map<String, Map<String, Double>> result = null;
        switch (valueType){
            case INT:{
                result = getIntRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                break;
            }
            case LONG:{
                result = getLongRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                break;
            }
            case FLOAT:{
                result = getFloatRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                break;
            }
            case DOUBLE:{
                result = getDoubleRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                break;
            }
        }
        return result;
    }

    private Double getSingleAggregationResult2(String host, String metric, long startTime, long endTime, String filter, AggregationType aggregationType, ValueType valueType) {

        Double result = 0d;
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add(host);
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add(metric);

        long startHour = (startTime % 3600000 == 0) ? (startTime / 3600000) : (startTime / 3600000 + 1);
        long endHour = endTime / 3600000;
        switch (aggregationType) {
            case MAX: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (startHour * 3600000 > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, startHour * 3600000, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > endHour * 3600000) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, endHour * 3600000, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = Math.max(firstIntervalResult, lastIntervalResult);
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for(AggregationData r : totoalResultSet){
                    totalTimeSlices.add(r.getTimeSlice()) ;
                }
                for(AggregationData r : ResultSet){
                    usedTimeSlices.add(r.getTimeSlice());
                    result = Math.max(result, r.getMaxValue());
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for(long time : totalTimeSlices){
                    System.out.println(time);
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time*3600000, time*3600000 + 3600000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result = Math.max(result, intervalResult);
                }
                break;
            }
            case MIN: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (startHour * 3600000 > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, startHour * 3600000, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? Long.MAX_VALUE : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = Long.MAX_VALUE;
                }

                double lastIntervalResult;
                if (endTime > endHour * 3600000) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, endHour * 3600000, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? Long.MAX_VALUE : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = Long.MAX_VALUE;
                }
                result = Math.min(firstIntervalResult, lastIntervalResult);
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for(AggregationData r : totoalResultSet){
                    totalTimeSlices.add(r.getTimeSlice()) ;
                }
                for(AggregationData r : ResultSet){
                    usedTimeSlices.add(r.getTimeSlice());
                    result = Math.min(result, r.getMinValue());
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for(long time : totalTimeSlices){
                    System.out.println(time);
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time*3600000, time*3600000 + 3600000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ?Long.MAX_VALUE : intervalMap.get(host).get(metric);
                    result = Math.min(result, intervalResult);
                }
                break;
            }
            case COUNT: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (startHour * 3600000 > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, startHour * 3600000, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > endHour * 3600000) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, endHour * 3600000, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = firstIntervalResult + lastIntervalResult;
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for(AggregationData r : totoalResultSet){
                    totalTimeSlices.add(r.getTimeSlice()) ;
                }
                for(AggregationData r : ResultSet){
                    usedTimeSlices.add(r.getTimeSlice());
                    result += r.getCountValue();
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for(long time : totalTimeSlices){
                    System.out.println(time);
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time*3600000, time*3600000 + 3600000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result += intervalResult;
                }
                break;
            }
            case SUM: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (startHour * 3600000 > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, startHour * 3600000, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > endHour * 3600000) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, endHour * 3600000, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = firstIntervalResult + lastIntervalResult;
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for(AggregationData r : totoalResultSet){
                    totalTimeSlices.add(r.getTimeSlice()) ;
                }
                for(AggregationData r : ResultSet){
                    usedTimeSlices.add(r.getTimeSlice());
                    result+= r.getSumValue();
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for(long time : totalTimeSlices){
                    System.out.println(time);
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time*3600000, time*3600000 + 3600000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result += intervalResult;
                }
                break;
            }
            case AVG:{
                double s = getSingleAggregationResult2(host, metric, startTime, endTime, filter, AggregationType.SUM, valueType);
                double c = getSingleAggregationResult2(host, metric, startTime, endTime, filter, AggregationType.COUNT, valueType);
                result = s / c;
                break;
            }
        }
        return result;
    }

    public Map<String, Map<String, Double>> getFloatAggregation(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType, ValueType valueType){
        //make sure that data has been pre-aggregated
        Map<String, Map<String, Double>> result = new HashMap<>();
        for(String host : hosts){
            Map<String, Double> hostResult = new HashMap<>();
            for(String metric : metrics){
                hostResult.put(metric, getSingleAggregationResult2(host, metric, startTime, endTime, filter, aggregationType, valueType));
            }
            result.put(host, hostResult);
        }
        return  result;
    }

    public void preAggregateFunction2(List<String> hosts, List<String> metrics, long startTime, long endTime, SagittariusWriter writer){

        // TODO: 17-3-28 if hosts == null then hosts = all hosts, so does metrics
        long startTimeHour = startTime/3600000;
        long endTimeHour = endTime/3600000;
        while (startTimeHour < endTimeHour){
            long queryStartTime = startTimeHour * 3600000;
            long queryEndTime = queryStartTime + 3600000;
            for (String host : hosts){
                for(String metric : metrics){
                    ArrayList<String> queryHost = new ArrayList<>();
                    queryHost.add(host);
                    ArrayList<String> queryMetric = new ArrayList<>();
                    queryMetric.add(metric);
                    String filter = getRangeQueryPredicates(queryHost,queryMetric, queryStartTime,queryEndTime).get(0);
                    SimpleStatement statement = new SimpleStatement("select max(value) from data_float where " + filter);
                    ResultSet resultSet = session.execute(statement);
                    double maxResult = (double)(resultSet.all().get(0).getFloat(0));
                    statement = new SimpleStatement("select min(value) from data_float where " + filter);
                    resultSet = session.execute(statement);
                    double minResult = (double)(resultSet.all().get(0).getFloat(0));
                    statement = new SimpleStatement("select count(value) from data_float where " + filter);
                    resultSet = session.execute(statement);
                    double countResult = (double)(resultSet.all().get(0).getLong(0));
                    statement = new SimpleStatement("select sum(value) from data_float where " + filter);
                    resultSet = session.execute(statement);
                    double sumResult = (double)(resultSet.all().get(0).getFloat(0));
                    if(countResult > 0){
                        writer.insert(host, metric, startTimeHour, maxResult, minResult, countResult, sumResult);
                    }

                }
            }
            startTimeHour += 1;
        }
    }
}