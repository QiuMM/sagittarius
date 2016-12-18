package com.sagittarius.model;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qmm on 2016/12/14.
 * create data tables and indexes in Cassandra
 */
public class ModelInit {
    private static final Logger logger = LoggerFactory.getLogger(ModelInit.class);

    public static void main(String[] args) {
        Cluster cluster = null;
        try {
            /**
             * connect cassandra cluster
             */
            SocketOptions socketOptions = new SocketOptions().setConnectTimeoutMillis(10000).setReadTimeoutMillis(30000);
            QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL);
            Cluster.Builder builder = Cluster.builder().addContactPoint(args[0]).withPort(Integer.parseInt(args[1])).withSocketOptions(socketOptions).withQueryOptions(queryOptions);
            if (args.length == 6) { //if credential is needed
                builder.withCredentials(args[4], args[5]);
            }
            cluster = builder.build();
            Session session = cluster.connect();
            /**
             * data model initial
             */
            session.execute(String.format(DataModel.createKeyspace, args[2], args[3]));
            session.execute("USE sagittarius");
            session.execute(DataModel.createTable_hostMetric);
            session.execute(DataModel.createTable_hostTags);
            session.execute(DataModel.createTable_owner);
            session.execute(DataModel.createTable_int);
            session.execute(DataModel.createTable_long);
            session.execute(DataModel.createTable_float);
            session.execute(DataModel.createTable_double);
            session.execute(DataModel.createTable_boolean);
            session.execute(DataModel.createTable_text);
            session.execute(DataModel.createTable_geo);
            session.execute(DataModel.createTable_latest_int);
            session.execute(DataModel.createTable_latest_long);
            session.execute(DataModel.createTable_latest_float);
            session.execute(DataModel.createTable_latest_double);
            session.execute(DataModel.createTable_latest_boolean);
            session.execute(DataModel.createTable_latest_text);
            session.execute(DataModel.createTable_latest_geo);
            session.execute(DataModel.createIndex_hostMetric);
            session.execute(DataModel.createIndex_hostTags);
            session.execute(DataModel.createIndex_owner);
            session.execute(DataModel.createIndex_int);
            session.execute(DataModel.createIndex_long);
            session.execute(DataModel.createIndex_float);
            session.execute(DataModel.createIndex_double);
            session.execute(DataModel.createIndex_boolean);
            session.execute(DataModel.createIndex_text);
            session.execute(DataModel.createIndex_geoLatitude);
            session.execute(DataModel.createIndex_geoLongitude);
            logger.info("Sagittarius time series data model initialized success. Congratulations!");
        } catch (Exception e) {
            logger.error("Sagittarius time series data model initialized fail. Please check the arguments!");
            e.printStackTrace();
        } finally {
            if (cluster != null) cluster.close();
        }
    }
}
