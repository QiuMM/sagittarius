package com.sagittarius.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.TypePartitionPair;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.cache.Cache;
import com.sagittarius.cache.LRUCache;
import com.sagittarius.read.Reader;
import com.sagittarius.read.SagittariusReader;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * client which expose interfaces to user
 */
public class SagittariusClient {
    private Session session;
    private MappingManager mappingManager;
    private JavaSparkContext sparkContext;
    private Cache<HostMetricPair, TypePartitionPair> cache;
    private Reader reader;
    private Writer writer;

    public SagittariusClient(Cluster cluster, SparkConf sparkConf, int cacheSize) {
        cluster.getConfiguration().getCodecRegistry()
                .register(new EnumNameCodec<>(TimePartition.class))
                .register(new EnumNameCodec<>(ValueType.class))
                .register(new SimpleTimestampCodec());
        this.session = cluster.connect("sagittarius");
        this.mappingManager = new MappingManager(session);
        this.sparkContext  = new JavaSparkContext(sparkConf);
        this.cache = new LRUCache<>(cacheSize);
        this.reader = new SagittariusReader(session, mappingManager, sparkContext, cache);
        this.writer = new SagittariusWriter(session, mappingManager);
    }

    public Session getSession() {
        return session;
    }

    public MappingManager getMappingManager() {
        return mappingManager;
    }

    public Reader getReader() {
        return reader;
    }

    public Writer getWriter() {
        return writer;
    }

    public void close() {
        if (session != null) {
            session.close();
            session = null;
        }
    }
}
