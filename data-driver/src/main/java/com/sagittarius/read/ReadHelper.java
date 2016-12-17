package com.sagittarius.read;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by qmm on 2016/12/17.
 */
public class ReadHelper {

    public static String generateInStatement(List<String> params) {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append("'").append(param).append("'").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static Map<String, List<String>> getDatePartedMetrics(Session session, MappingManager mappingManager, List<String> hosts, List<String> metrics, long time) {
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Map<String, List<String>> dateMetrics = new HashMap<>();

        Result<HostMetric> hostMetrics = getHostMetrics(session, mapper, hosts, metrics);
        for (HostMetric hostMetric : hostMetrics) {
            String date = TimeUtil.getDate(time, hostMetric.getDateInterval());
            if (dateMetrics.containsKey(date)) {
                dateMetrics.get(date).add(hostMetric.getMetric());
            } else {
                List<String> metricList = new ArrayList<>();
                metricList.add(hostMetric.getMetric());
                dateMetrics.put(date, metricList);
            }
        }

        return dateMetrics;
    }

    private static Result<HostMetric> getHostMetrics(Session session, Mapper<HostMetric> mapper, List<String> hosts, List<String> metrics) {
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRIC_QUERY_STATEMENT, generateInStatement(hosts), generateInStatement(metrics)));
        ResultSet rs = session.execute(statement);
        return mapper.map(rs);
    }
}
