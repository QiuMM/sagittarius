package com.sagittarius.model;

/**
 * Created by qmm on 2016/12/14.
 * time series data model, it becomes DDL statement in Cassandra
 */
public class DataModel {
    public static final String createKeyspace = "CREATE KEYSPACE sagittarius WITH replication = {'class': '%s', 'replication_factor': '%s'}";

    public static final String createTable_hostMetric = "CREATE TABLE host_metric (host text, metric text, value_type text, date_interval text, primary key(host, metric))";
    public static final String createTable_hostTags = "CREATE TABLE host_tags (host text, tags map<text, text>, primary key(host))";
    public static final String createTable_owner = "CREATE TABLE owner (user text, host text, primary key(user, host))";

    public static final String createTable_int = "CREATE TABLE data_int (host text, metric text, date text, created_at timestamp, received_at timestamp, value int, primary key((host, metric, date), received_at))";
    public static final String createTable_long = "CREATE TABLE data_long (host text, metric text, date text, created_at timestamp, received_at timestamp, value bigint, primary key((host, metric, date), received_at))";
    public static final String createTable_float = "CREATE TABLE data_float (host text, metric text, date text, created_at timestamp, received_at timestamp, value float, primary key((host, metric, date), received_at))";
    public static final String createTable_double = "CREATE TABLE data_double (host text, metric text, date text, created_at timestamp, received_at timestamp, value double, primary key((host, metric, date), received_at))";
    public static final String createTable_boolean = "CREATE TABLE data_boolean (host text, metric text, date text, created_at timestamp, received_at timestamp, value boolean, primary key((host, metric, date), received_at))";
    public static final String createTable_text = "CREATE TABLE data_text (host text, metric text, date text, created_at timestamp, received_at timestamp, value text, primary key((host, metric, date), received_at))";
    public static final String createTable_geo = "CREATE TABLE data_geo (host text, metric text, date text, created_at timestamp, received_at timestamp, latitude float, longitude float, primary key((host, metric, date), received_at))";

    public static final String createTable_latest_int = "CREATE TABLE latest_int (host text, metric text, created_at timestamp, received_at timestamp, value int, primary key((host, metric)))";
    public static final String createTable_latest_long = "CREATE TABLE latest_long (host text, metric text, created_at timestamp, received_at timestamp, value bigint, primary key((host, metric)))";
    public static final String createTable_latest_float = "CREATE TABLE latest_float (host text, metric text, created_at timestamp, received_at timestamp, value float, primary key((host, metric)))";
    public static final String createTable_latest_double = "CREATE TABLE latest_double (host text, metric text, created_at timestamp, received_at timestamp, value double, primary key((host, metric)))";
    public static final String createTable_latest_boolean = "CREATE TABLE latest_boolean (host text, metric text, created_at timestamp, received_at timestamp, value boolean, primary key((host, metric)))";
    public static final String createTable_latest_text = "CREATE TABLE latest_text (host text, metric text, created_at timestamp, received_at timestamp, value text, primary key((host, metric)))";
    public static final String createTable_latest_geo = "CREATE TABLE latest_geo (host text, metric text, created_at timestamp, received_at timestamp, latitude float, longitude float, primary key((host, metric)))";

    public static final String createIndex_hostMetric = "CREATE INDEX ON host_metric (metric)";
    public static final String createIndex_hostTags = "CREATE INDEX ON host_tags (ENTRIES(tags))";
    public static final String createIndex_owner = "CREATE INDEX ON owner (host)";

    public static final String createIndex_int = "CREATE INDEX ON data_int (value)";
    public static final String createIndex_long = "CREATE INDEX ON data_long (value)";
    public static final String createIndex_float = "CREATE INDEX ON data_float (value)";
    public static final String createIndex_double = "CREATE INDEX ON data_double (value)";
    public static final String createIndex_boolean = "CREATE INDEX ON data_boolean (value)";
    public static final String createIndex_text = "CREATE INDEX ON data_text (value)";
    public static final String createIndex_geoLatitude = "CREATE INDEX ON data_geo (latitude)";
    public static final String createIndex_geoLongitude = "CREATE INDEX ON data_geo (longitude)";
}

