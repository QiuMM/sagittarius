package com.sagittarius.example;

import com.datastax.driver.core.*;
import com.sagittarius.bean.bulk.BulkDoubleData;
import com.sagittarius.bean.bulk.BulkIntData;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.bean.result.IntPoint;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.read.Reader;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.LocalDate;
import java.util.*;

public class Example {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) {
        CassandraConnection connection = CassandraConnection.getInstance();
        Cluster cluster = connection.getCluster();
        SagittariusClient client = new SagittariusClient(cluster);
        Writer writer = client.getWriter();
        //registerHostMetricInfo(writer);
        //registerHostTags(writer);
        //registerOwnerInfo(writer);
        insert(writer);
        //writeTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //batchWriteTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));

        //insertLoop(writer);
        Reader reader = client.getReader();
        read(reader);
        //readbyRange(reader);
        //readFuzzy(reader);

    }

    private static void writeTestAsync(Writer writer, int threads, int runTime) {
        long start = System.currentTimeMillis();
        long time = System.currentTimeMillis();
        Random random = new Random();
        long consumeTime = 0;
        long count = 0;
        double throughput = 0;
        while ((System.currentTimeMillis() - start) < runTime * 60 * 60 * 1000) {
            BulkDoubleData datas = new BulkDoubleData();
            for (int i = 0; i < 50; ++i) {
                for (int j = 0; j < 100; ++j) {
                    datas.addData("12828" + i, "APP", time, time, TimePartition.DAY, random.nextDouble() * 100);
                    ++time;
                }
            }
            long startTime = System.currentTimeMillis();
            writer.bulkInsert(datas, threads);
            consumeTime += System.currentTimeMillis() - startTime;
            count += 5000;
            throughput = count / ((double) consumeTime / 1000);
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void batchWriteTest(Writer writer, int threads, int runTime, int batchSize) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchWriteTask> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchWriteTask task = new BatchWriteTask(writer, host, runTime, batchSize);
            task.start();
            tasks.add(task);
        }

        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            double throughput = 0;
            long count = 0;
            for (BatchWriteTask task : tasks) {
                throughput += task.getThroughput();
                count += task.getCount();
            }
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void writeTest(Writer writer, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<WriteTask> tasks = new ArrayList<>();
        for (String host : hosts) {
            WriteTask task = new WriteTask(writer, host, runTime);
            task.start();
            tasks.add(task);
        }

        /*final LoadBalancingPolicy loadBalancingPolicy =
                cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
        final PoolingOptions poolingOptions =
                cluster.getConfiguration().getPoolingOptions();

        ScheduledExecutorService scheduled =
                Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Session.State state = client.getSession().getState();
                for (Host host : state.getConnectedHosts()) {
                    HostDistance distance = loadBalancingPolicy.distance(host);
                    int connections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    System.out.printf("%s connections=%d, current load=%d, maxload=%d%n",
                            host, connections, inFlightQueries,
                            connections * poolingOptions.getMaxRequestsPerConnection(distance));
                }
            }
        }, 5, 5, TimeUnit.SECONDS);*/

        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            double throughput = 0;
            long count = 0;
            for (WriteTask task : tasks) {
                throughput += task.getThroughput();
                count += task.getCount();
            }
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void read(Reader reader) {
        List<String> hosts = new ArrayList<>();
        hosts.add("128280");
        hosts.add("128290");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        Map<String, List<DoublePoint>> result = reader.getDoubleLatest(hosts, metrics);
        //Map<String, List<DoublePoint>> result = reader.getDoublePoint(hosts, metrics, 1482319512851L);
        for (Map.Entry<String, List<DoublePoint>> entry : result.entrySet()) {
            System.out.println(entry.getKey());
            for (DoublePoint point : entry.getValue()) {
                System.out.println(point.getMetric() + " " + point.getPrimaryTime()+ " " + point.getValue());
            }
        }
    }
    private static void readbyRange(Reader reader) {
        List<String> hosts = new ArrayList<>();
        //hosts.add("131980");
        System.out.println("查找");
        hosts.add("128280");
        hosts.add("1282835");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        LocalDateTime start = LocalDateTime.of(1993,10,11,0,0);
        LocalDateTime end = LocalDateTime.of(1993,10,14,5,59);
        Map<String, List<DoublePoint>> result = reader.getDoubleRange(hosts, metrics,start.toEpochSecond(ZoneOffset.UTC)*1000,end.toEpochSecond(ZoneOffset.UTC)*1000);
        //Map<String, List<DoublePoint>> result = reader.getDoublePoint(hosts, metrics, 1482319512851L);
        for (Map.Entry<String, List<DoublePoint>> entry : result.entrySet()) {
            System.out.println(entry.getKey());
            for (DoublePoint point : entry.getValue()) {
                System.out.println(point.getMetric() + " " + point.getPrimaryTime()+" "+ LocalDateTime.ofEpochSecond(point.getPrimaryTime()/1000,0,ZoneOffset.UTC) + " " + point.getValue());
            }
        }
    }

    private static void readFuzzy(Reader reader) {

        String host="128280";
        String metric="APP";
        DoublePoint point =  reader.getFuzzyDoublePoint(host,metric,1483712410000L, Shift.NEAREST);
        System.out.println(point.getMetric() + " " + point.getPrimaryTime() + " " + point.getValue());

    }

    private static void insertLoop(Writer writer) {
        LocalDateTime start = LocalDateTime.of(1993,10,12,0,0);
        LocalDateTime end = LocalDateTime.of(1993,10,14,23,59);
        System.out.println("插入");
        while (!start.isAfter(end)) {
            double value=Math.random()*100;
            writer.insert("1282835", "APP", start.toEpochSecond(ZoneOffset.UTC)*1000, start.toEpochSecond(ZoneOffset.UTC)*1000, TimePartition.DAY,value );
            System.out.println("APP" + " " + start.toEpochSecond(ZoneOffset.UTC)*1000+" "+ start.toString()+ " " + value);
            start = start.plusHours(6);
        }
    }

    private static void insert(Writer writer) {
        long time1 = System.currentTimeMillis();
        long time2 = System.currentTimeMillis();
        System.out.println(time1);
        System.out.println(time2);
        writer.insert("128280", "APP", time1, -1, TimePartition.DAY, 5.20d);

    }

    private static void registerHostMetricInfo(Writer writer) {
        MetricMetadata metricMetadata1 = new MetricMetadata("APP", TimePartition.DAY, ValueType.DOUBLE, "加速踏板位置");
        MetricMetadata metricMetadata2 = new MetricMetadata("ECT", TimePartition.DAY, ValueType.INT, "发动机冷却液温度");
        List<MetricMetadata> metricMetadatas = new ArrayList<>();
        metricMetadatas.add(metricMetadata1);
        metricMetadatas.add(metricMetadata2);
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            hosts.add("12828" + i);
        }
        for (String host : hosts) {
            long time = System.currentTimeMillis();
            writer.registerHostMetricInfo(host, metricMetadatas);
            logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
        }
    }

    private static void registerOwnerInfo(Writer writer) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            hosts.add("12828" + i);
        }
        for (String host : hosts) {
            writer.registerOwnerInfo("Qiu Mingming", hosts);
        }
    }

    private static void registerHostTags(Writer writer) {
        Map<String, String> tags = new HashMap<>();
        tags.put("price", "¥.10000");
        writer.registerHostTags("128280", tags);
    }
}
