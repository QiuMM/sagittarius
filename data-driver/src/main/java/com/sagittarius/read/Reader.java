package com.sagittarius.read;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.sagittarius.bean.query.*;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.read.interfaces.IReader;
import com.sagittarius.util.TimeUtil;

import java.nio.ByteBuffer;
import java.util.*;

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

    private String getTableByType(HostMetric.ValueType valueType) {
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

    private ResultSet getPointResultSet(List<String> hosts, List<String> metrics, long time, HostMetric.ValueType valueType) {
        String table = getTableByType(valueType);
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> dateHostMetric = ReadHelper.getDatePartedHostMetric(hostMetrics, time);
        BatchStatement batchStatement = new BatchStatement();
        for (Map.Entry<String, Map<String, Set<String>>> entry : dateHostMetric.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), entry.getKey(), time));
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
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
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private ResultSet getLatestResultSet(List<String> hosts, List<String> metrics, HostMetric.ValueType valueType) {
        String table = null;
        switch (valueType) {
            case INT:
                table = "latest_int";
                break;
            case LONG:
                table = "latest_long";
                break;
            case FLOAT:
                table = "latest_float";
                break;
            case DOUBLE:
                table = "latest_double";
                break;
            case BOOLEAN:
                table = "latest_boolean";
                break;
            case STRING:
                table = "latest_text";
                break;
            case GEO:
                table = "latest_geo";
                break;
        }

        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_QUERY_STATEMENT, table, ReadHelper.generateInStatement(hosts), ReadHelper.generateInStatement(metrics)));
        return session.execute(statement);
    }

    @Override
    public Map<String, List<IntPoint>> getIntLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.INT);
        Mapper<IntLatest> mapper = mappingManager.mapper(IntLatest.class);
        Result<IntLatest> latests = mapper.map(rs);

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.LONG);
        Mapper<LongLatest> mapper = mappingManager.mapper(LongLatest.class);
        Result<LongLatest> latests = mapper.map(rs);

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.FLOAT);
        Mapper<FloatLatest> mapper = mappingManager.mapper(FloatLatest.class);
        Result<FloatLatest> latests = mapper.map(rs);

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.DOUBLE);
        Mapper<DoubleLatest> mapper = mappingManager.mapper(DoubleLatest.class);
        Result<DoubleLatest> latests = mapper.map(rs);

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.BOOLEAN);
        Mapper<BooleanLatest> mapper = mappingManager.mapper(BooleanLatest.class);
        Result<BooleanLatest> latests = mapper.map(rs);

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.STRING);
        Mapper<StringLatest> mapper = mappingManager.mapper(StringLatest.class);
        Result<StringLatest> latests = mapper.map(rs);

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(latest.getMetric(), latest.getReceivedAt(), latest.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics) {
        ResultSet rs = getLatestResultSet(hosts, metrics, HostMetric.ValueType.GEO);
        Mapper<GeoLatest> mapper = mappingManager.mapper(GeoLatest.class);
        Result<GeoLatest> latests = mapper.map(rs);

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(latest.getMetric(), latest.getReceivedAt(), latest.getLatitude(), latest.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(latest.getMetric(), latest.getReceivedAt(), latest.getLatitude(), latest.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private List<String> getRangeQueryString(List<String> hosts, List<String> metrics, long startTime, long endTime, HostMetric.ValueType valueType) {
        String table = getTableByType(valueType);
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> dateHostMetric = ReadHelper.getDatePartedHostMetric(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();

        for (Map.Entry<String, Map<String, Set<String>>> entry : dateHostMetric.entrySet()) {
            String startDate = entry.getKey();
            if (startDate.contains("D")) {
                String endDate = TimeUtil.getDate(endTime, HostMetric.DateInterval.DAY);
                String dateHead = startDate.substring(0, startDate.indexOf("D") + 1);
                int startDay = Integer.parseInt(startDate.substring(startDate.indexOf("D") + 1));
                int endDay = Integer.parseInt(endDate.substring(startDate.indexOf("D") + 1));
                if (endDay > startDay) {
                    for (int i = startDay + 1; i < endDay; ++i) {
                        String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), dateHead + i);
                        querys.add(query);
                    }
                    String startQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), startDate, ">", startTime);
                    String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), endDate, "<", endTime);
                    querys.add(startQuery);
                    querys.add(endQuery);
                }
            }
        }

        return querys;
    }

    @Override
    public Map<String, List<IntPoint>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, NumericFilter filter, AggregationType aggregationType) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.INT);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        Result<IntData> datas = mapper.map(rs);

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, NumericFilter filter, AggregationType aggregationType) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.LONG);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        Result<LongData> datas = mapper.map(rs);

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, NumericFilter filter, AggregationType aggregationType) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.FLOAT);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        Result<FloatData> datas = mapper.map(rs);

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, NumericFilter filter, AggregationType aggregationType) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.DOUBLE);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        Result<DoubleData> datas = mapper.map(rs);

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, BooleanFilter filter) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.BOOLEAN);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        Result<BooleanData> datas = mapper.map(rs);

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, StringFilter filter) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.STRING);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        Result<StringData> datas = mapper.map(rs);

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getReceivedAt(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, GeoFilter filter) {
        List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, HostMetric.ValueType.GEO);
        BatchStatement batchStatement = new BatchStatement();
        for (String query : querys) {
            batchStatement.add(new SimpleStatement(query));
        }
        ResultSet rs = session.execute(batchStatement);
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        Result<GeoData> datas = mapper.map(rs);

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getReceivedAt(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }
}
