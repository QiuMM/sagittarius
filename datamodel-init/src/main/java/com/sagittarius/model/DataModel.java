package com.sagittarius.model;

/**
 * Created by qmm on 2016/12/14.
 * time series data model, it becomes DDL statement in Cassandra
 */
public class DataModel {
    public static final String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS sagittarius WITH replication = {'class': '%s', 'replication_factor': '%s'}";

    public static final String createTable_hostMetric = "CREATE TABLE IF NOT EXISTS host_metric (host text, metric text, value_type text, date_interval text, primary key(host, metric))";
    public static final String createTable_hostTags = "CREATE TABLE IF NOT EXISTS host_tags (host text, tags map<text, text>, primary key(host))";
    public static final String createTable_owner = "CREATE TABLE IF NOT EXISTS owner (user text, host text, primary key(user, host))";

    public static final String createTable_int = "CREATE TABLE IF NOT EXISTS data_int (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value int, primary key((host, metric, date), primary_time))";
    public static final String createTable_long = "CREATE TABLE IF NOT EXISTS data_long (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value bigint, primary key((host, metric, date), primary_time))";
    public static final String createTable_float = "CREATE TABLE IF NOT EXISTS data_float (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value float, primary key((host, metric, date), primary_time))";
    public static final String createTable_double = "CREATE TABLE IF NOT EXISTS data_double (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value double, primary key((host, metric, date), primary_time))";
    public static final String createTable_boolean = "CREATE TABLE IF NOT EXISTS data_boolean (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value boolean, primary key((host, metric, date), primary_time))";
    public static final String createTable_text = "CREATE TABLE IF NOT EXISTS data_text (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, value text, primary key((host, metric, date), primary_time))";
    public static final String createTable_geo = "CREATE TABLE IF NOT EXISTS data_geo (host text, metric text, date text, primary_time timestamp, secondary_time timestamp, latitude float, longitude float, primary key((host, metric, date), primary_time))";

    public static final String createTable_latest_int = "CREATE TABLE IF NOT EXISTS latest_int (host text, metric text, primary_time timestamp, secondary_time timestamp, value int, primary key((host, metric)))";
    public static final String createTable_latest_long = "CREATE TABLE IF NOT EXISTS latest_long (host text, metric text, primary_time timestamp, secondary_time timestamp, value bigint, primary key((host, metric)))";
    public static final String createTable_latest_float = "CREATE TABLE IF NOT EXISTS latest_float (host text, metric text, primary_time timestamp, secondary_time timestamp, value float, primary key((host, metric)))";
    public static final String createTable_latest_double = "CREATE TABLE IF NOT EXISTS latest_double (host text, metric text, primary_time timestamp, secondary_time timestamp, value double, primary key((host, metric)))";
    public static final String createTable_latest_boolean = "CREATE TABLE IF NOT EXISTS latest_boolean (host text, metric text, primary_time timestamp, secondary_time timestamp, value boolean, primary key((host, metric)))";
    public static final String createTable_latest_text = "CREATE TABLE IF NOT EXISTS latest_text (host text, metric text, primary_time timestamp, secondary_time timestamp, value text, primary key((host, metric)))";
    public static final String createTable_latest_geo = "CREATE TABLE IF NOT EXISTS latest_geo (host text, metric text, primary_time timestamp, secondary_time timestamp, latitude float, longitude float, primary key((host, metric)))";

    public static final String createIndex_hostMetric = "CREATE INDEX IF NOT EXISTS ON host_metric (metric)";
    public static final String createIndex_hostTags = "CREATE INDEX IF NOT EXISTS ON host_tags (ENTRIES(tags))";
    public static final String createIndex_owner = "CREATE INDEX IF NOT EXISTS ON owner (host)";

    public static final String createIndex_int = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_int (value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'PREFIX'}";
    public static final String createIndex_long = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_long (value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'PREFIX'}";
    public static final String createIndex_float = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_float (value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'PREFIX'}";
    public static final String createIndex_double = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_double (value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'}";
    public static final String createIndex_boolean = "CREATE INDEX IF NOT EXISTS ON data_boolean (value)";
    public static final String createIndex_text = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_text (value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'}";
    public static final String createIndex_geoLatitude = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_geo (latitude) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'}";
    public static final String createIndex_geoLongitude = "CREATE CUSTOM INDEX IF NOT EXISTS ON data_geo (longitude) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'}";
}

