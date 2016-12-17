package com.sagittarius.read;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.read.interfaces.IReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by qmm on 2016/12/15.
 */
public class Reader implements IReader {
    private MappingManager mappingManager;
    private Session session;

    public Reader(Session session, MappingManager mappingManager) {
        this.session = session;
        this.mappingManager = mappingManager;
    }

    public static enum AggregationType {
        MIN, MAX, AVG, SUM, COUNT
    }

    private ResultSet getPointResultSet(List<String> hosts, List<String> metrics, long time, HostMetric.ValueType valueType) {
        String table = null;
        Mapper mapper = null;
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

        Map<String, List<String>> dateMetrics = ReadHelper.getDatePartedMetrics(session, mappingManager, hosts, metrics, time);
        BatchStatement batchStatement = new BatchStatement();
        for (Map.Entry<String, List<String>> entry : dateMetrics.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, ReadHelper.generateInStatement(hosts), ReadHelper.generateInStatement(entry.getValue()), entry.getKey(), time));
            batchStatement.add(statement);
        }

        return session.execute(batchStatement);
    }

    @Override
    public Map<String, List<IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.INT);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        Result<IntData> datas = mapper.map(rs);

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.LONG);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        Result<LongData> datas = mapper.map(rs);

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.FLOAT);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        Result<FloatData> datas = mapper.map(rs);

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.DOUBLE);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        Result<DoubleData> datas = mapper.map(rs);

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.BOOLEAN);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        Result<BooleanData> datas = mapper.map(rs);

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.STRING);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        Result<StringData> datas = mapper.map(rs);

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long time) {
        ResultSet rs = getPointResultSet(hosts, metrics, time, HostMetric.ValueType.GEO);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        Result<GeoData> datas = mapper.map(rs);

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoData data : datas) {
            if (result.containsKey(data.getHost())) {
                result.get(data.getHost()).add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
                result.put(data.getHost(), points);
            }
        }

        return result;
    }
}
