package com.sagittarius.example;

import com.sagittarius.core.SagittariusClient;
import com.sagittarius.read.interfaces.IReader;
import com.sagittarius.write.interfaces.IWriter;

/**
 * Created by qmm on 2016/12/20.
 */
public class Example {
    public static void main(String[] args) {
        CassandraConnection connection = CassandraConnection.getInstance();
        SagittariusClient client = SagittariusClient.createInstance(connection.getCluster());
        IWriter writer = client.getWriter();
        IReader reader = client.getReader();
    }
}
