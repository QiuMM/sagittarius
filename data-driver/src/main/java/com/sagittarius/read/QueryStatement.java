package com.sagittarius.read;

/**
 * Created by qmm on 2016/12/16.
 */
public class QueryStatement {
    static final String HOST_METRIC_QUERY_STATEMENT = "select * from host_metric where host in (%s) and metric in (%s)";
    static final String POINT_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and date='%s' and received_at=%d";
    static final String LATEST_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s)";

    static final String WHOLE_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and date='%s'";
    static final String PARTIAL_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and date='%s' and received_at%s%d";
}
