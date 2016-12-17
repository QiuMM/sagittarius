package com.sagittarius.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.read.Reader;
import com.sagittarius.write.Writer;

/**
 * Created by qmm on 2016/12/15.
 */
public class SagittariusClient {
    private Cluster cluster;
    private Session session;
    private MappingManager mappingManager;

    private SagittariusClient(Cluster cluster) {
        this.cluster = cluster;
        cluster.getConfiguration().getCodecRegistry().register(new EnumNameCodec<HostMetric.DateInterval>(HostMetric.DateInterval.class)).register(new EnumNameCodec<HostMetric.ValueType>(HostMetric.ValueType.class));
        this.session = cluster.connect("sagittarius");
        this.mappingManager = new MappingManager(session);
    }

    public static SagittariusClient createInstance(Cluster cluster) {
        return new SagittariusClient(cluster);
    }

    public Writer getWriter() {
        return new Writer(session, mappingManager);
    }

    public Reader getReader() {
        return new Reader(session, mappingManager);
    }

    public void close() {
        if (session != null) {
            session.close();
            session = null;
        }
    }
}
