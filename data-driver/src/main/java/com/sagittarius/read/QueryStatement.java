package com.sagittarius.read;

public class QueryStatement {
    static final String HOST_METRICS_QUERY_STATEMENT = "select * from host_metric where host in (%s) and metric in (%s)";
    static final String HOST_METRIC_QUERY_STATEMENT = "select * from host_metric where host='%s' and metric='%s'";
    static final String POINT_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time=%d";
    static final String POINT_BEFORE_SHIFT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' and primary_time<=%d order by primary_time DESC limit 1 ";
    static final String POINT_AFTER_SHIFT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' and primary_time>=%d order by primary_time limit 1";
    static final String LATEST_TIMESLICE_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s)";
    static final String LATEST_POINT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' order by primary_time DESC limit 1 ";

    static final String WHOLE_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s'";
    static final String PARTIAL_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time%s%d";
    static final String IN_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time>=%d and primary_time<=%d";

}
