package com.sagittarius.example;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class CassandraConnection {

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);
    private static String[] cassandraNodes = new String[]{"192.168.3.17","192.168.3.19","192.168.3.21","192.168.3.23","192.168.3.25"};
//    private static String[] cassandraNodes = new String[]{"192.168.3.52"};
    private static int cassandraPort = 9042;
    private static CassandraConnection cassandraConnection = null;
    private Cluster cluster;

    private CassandraConnection() {
        init();
    }

    public static CassandraConnection getInstance() {
        if (cassandraConnection == null) {
            synchronized (CassandraConnection.class) {
                if (cassandraConnection == null) {
                    cassandraConnection = new CassandraConnection();
                }
            }
        }
        return cassandraConnection;
    }

    private void init() {

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL, 3, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 3, 10)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 4096)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 4096)
                .setHeartbeatIntervalSeconds(0);

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(60000);

        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String ip : cassandraNodes) {
            try {
                addresses.add(new InetSocketAddress(ip, cassandraPort));
            } catch (Exception e) {
                logger.error("ip or port has something wrong：{}", e.getMessage());
            }
        }

        cluster = Cluster.builder().addContactPointsWithPorts(addresses)
                .withPoolingOptions(poolingOptions)
               // .withSocketOptions(socketOptions)
                .build();

        try {
            /**
             * print cluster info
             */
            Metadata metadata = cluster.getMetadata();
            logger.info("Connect to cluster: {}", metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
                logger.info("Datatacenter: {}; Host: {}; Rack: {}",
                        host.getDatacenter(), host.getAddress(), host.getRack());
            }
        } catch (Exception e) {
            logger.error("Connect to Cassandra cluster fail：{}", e.getMessage());
        }
    }

    /**
     * close connection
     */
    public void close() {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        cassandraConnection = null;

        logger.info("Cassandra connection closed！");
    }

    public Cluster getCluster() {
        return cluster;
    }
}
