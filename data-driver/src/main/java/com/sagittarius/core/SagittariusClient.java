package com.sagittarius.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.read.Reader;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;

/**
 * client which expose interfaces to user
 */
public class SagittariusClient {
    private Session session;
    private MappingManager mappingManager;

    public SagittariusClient(Cluster cluster) {
        cluster.getConfiguration().getCodecRegistry()
                .register(new EnumNameCodec<>(TimePartition.class))
                .register(new EnumNameCodec<>(ValueType.class))
                .register(new SimpleTimestampCodec());
        this.session = cluster.connect("sagittarius");
        this.mappingManager = new MappingManager(session);
    }

    public Session getSession() {
        return session;
    }

    public Writer getWriter() {
        return new SagittariusWriter(session, mappingManager);
    }

    //public Reader getReader() {
        //return new SagittariusReader(session, mappingManager);
    //}

    public void close() {
        if (session != null) {
            session.close();
            session = null;
        }
    }
}
