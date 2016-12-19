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

import java.util.*;

/**
 * Created by qmm on 2016/12/17.
 */
public class ReadHelper {

    public static String generateInStatement(Collection<String> params) {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append("'").append(param).append("'").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static Map<String, Map<String, Set<String>>> getDatePartedHostMetric(Result<HostMetric> hostMetrics, long time) {
        Map<String, Map<String, Set<String>>> dateHostMetric = new HashMap<>();

        for (HostMetric hostMetric : hostMetrics) {
            String date = TimeUtil.getDate(time, hostMetric.getDateInterval());
            if (dateHostMetric.containsKey(date)) {
                Map<String, Set<String>> setMap = dateHostMetric.get(date);
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
                dateHostMetric.put(date, setMap);
            }
        }

        return dateHostMetric;
    }

    public static Result<HostMetric> getHostMetrics(Session session, Mapper<HostMetric> mapper, List<String> hosts, List<String> metrics) {
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRIC_QUERY_STATEMENT, generateInStatement(hosts), generateInStatement(metrics)));
        ResultSet rs = session.execute(statement);
        return mapper.map(rs);
    }
}
