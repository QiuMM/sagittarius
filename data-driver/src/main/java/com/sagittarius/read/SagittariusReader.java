package com.sagittarius.read;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.*;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.util.TimeUtil;

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

    private List<ResultSet> getPointResultSet(List<String> hosts, List<String> metrics, long Time, ValueType valueType) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> TimeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetric(hostMetrics, Time);
        for (Map.Entry<String, Map<String, Set<String>>> entry : TimeSliceHostMetric.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), entry.getKey(), Time));
            ResultSet set = session.execute(statement);
            resultSets.add(set);
        }
        return resultSets;
    }

    @Override
    public Map<String, List<IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.INT);
        List<IntData> datas = new ArrayList<>();
        Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.LONG);
        List<LongData> datas = new ArrayList<>();
        Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<LongPoint>> result = new HashMap<>();
        for (LongData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.FLOAT);
        List<FloatData> datas = new ArrayList<>();
        Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<FloatPoint>> result = new HashMap<>();
        for (FloatData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.DOUBLE);
        List<DoubleData> datas = new ArrayList<>();
        Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<DoublePoint>> result = new HashMap<>();
        for (DoubleData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.BOOLEAN);
        List<BooleanData> datas = new ArrayList<>();
        Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<BooleanPoint>> result = new HashMap<>();
        for (BooleanData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.STRING);
        List<StringData> datas = new ArrayList<>();
        Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<StringPoint>> result = new HashMap<>();
        for (StringData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
                result.put(host, points);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long Time, Shift shift) {
        List<ResultSet> resultSets = getPointResultSet(hosts, metrics, Time, ValueType.GEO);
        List<GeoData> datas = new ArrayList<>();
        Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
        for (ResultSet rs : resultSets) {
            datas.addAll(mapper.map(rs).all());
        }

        Map<String, List<GeoPoint>> result = new HashMap<>();
        for (GeoData data : datas) {
            String host = data.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }

    private ResultSet getLatestResultSet(List<String> hosts, List<String> metrics, ValueType valueType) {
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
        /*ResultSet rs = getLatestResultSet(hosts, metrics, ValueType.INT);
        Mapper<IntLatest> mapper = mappingManager.mapper(IntLatest.class);
        Result<IntLatest> latests = mapper.map(rs);

        Map<String, List<IntPoint>> result = new HashMap<>();
        for (IntLatest latest : latests) {
            String host = latest.getHost();
            if (result.containsKey(host)) {
                result.get(host).add(new IntPoint(latest.getMetric(), latest.getPrimaryTime(), latest.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(latest.getMetric(), latest.getPrimaryTime(), latest.getValue()));
                result.put(host, points);
            }
        }*/

        return null;
    }

    @Override
    public Map<String, List<LongPoint>> getLongLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    @Override
    public Map<String, List<FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    @Override
    public Map<String, List<DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    @Override
    public Map<String, List<BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    @Override
    public Map<String, List<StringPoint>> getStringLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    @Override
    public Map<String, List<GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics) {
        return null;
    }

    private List<String> getRangeQueryString(List<String> hosts, List<String> metrics, long startTime, long endTime, ValueType valueType) {
        String table = getTableByType(valueType);
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Result<HostMetric> hostMetrics = ReadHelper.getHostMetrics(session, mapper, hosts, metrics);
        Map<String, Map<String, Set<String>>> TimeSliceHostMetric = ReadHelper.getTimeSlicePartedHostMetric(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();

        for (Map.Entry<String, Map<String, Set<String>>> entry : TimeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            if (startTimeSlice.contains("D")) {
                String endTimeSlice = TimeUtil.generateTimeSlice(endTime, TimePartition.DAY);
                String TimeSliceHead = startTimeSlice.substring(0, startTimeSlice.indexOf("D") + 1);
                int startDay = Integer.parseInt(startTimeSlice.substring(startTimeSlice.indexOf("D") + 1));
                int endDay = Integer.parseInt(endTimeSlice.substring(startTimeSlice.indexOf("D") + 1));
                if (endDay > startDay) {
                    for (int i = startDay + 1; i < endDay; ++i) {
                        String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), TimeSliceHead + i);
                        querys.add(query);
                    }
                    String startQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), startTimeSlice, ">", startTime);
                    String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, ReadHelper.generateInStatement(entry.getValue().get("hosts")), ReadHelper.generateInStatement(entry.getValue().get("metrics")), endTimeSlice, "<", endTime);
                    querys.add(startQuery);
                    querys.add(endQuery);
                }
            }
        }

        return querys;
    }

    @Override
    public Map<String, List<IntPoint>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime) {
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
                result.get(host).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getValue()));
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
                result.get(host).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getLatitude(), data.getLongitude()));
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.getSecondaryTime(), data.getLatitude(), data.getLongitude()));
                result.put(host, points);
            }
        }

        return result;
    }
}