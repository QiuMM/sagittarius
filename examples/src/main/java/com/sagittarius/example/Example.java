package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.Filter;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.read.Reader;
import com.sagittarius.read.SagittariusReader;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.Writer;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Example {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) {
        CassandraConnection connection = CassandraConnection.getInstance();
        Cluster cluster = connection.getCluster();

        long time = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://192.168.3.17:7077").setAppName("test");
        //to fix the can't assign from .. to .. Error
        //String[] jars = {"examples-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        //sparkConf.setJars(jars);
        sparkConf.set("spark.cassandra.connection.host", "192.168.3.17");
        sparkConf.set("spark.cassandra.connection.port", "9042");
        //sparkConf.set("spark.driver.maxResultSize", "20g");
        SagittariusClient client = new SagittariusClient(cluster, sparkConf);
        Writer writer = client.getWriter();
        SagittariusReader reader = (SagittariusReader)client.getReader();
        //reader.test();
        floatRead(reader);
        logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
        //registerHostMetricInfo(writer);
        //registerHostTags(writer);
        //registerOwnerInfo(writer);
        //insert(writer);
        //writeTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //batchWriteTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
//        batchWriteTest(writer, 1, 1, 1000);
        //insertLoop(writer);

        //read(reader);
        //readbyRange(reader);
        //readFuzzy(reader);
        //floatRead(reader);
        //exit(0);

    }

    private static void floatRead(Reader reader){
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128998");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("发动机转速");
        long start = LocalDateTime.of(2017,2,26,0,0).toEpochSecond(TimeUtil.zoneOffset)*1000;
        long end = LocalDateTime.of(2017,2,27,23,59).toEpochSecond(TimeUtil.zoneOffset)*1000;
        String filter = "value >= 33 and value <= 34";
        Map<String, Map<String, List<FloatPoint>>> result = reader.getFloatRange(hosts, metrics, start, end, filter);
        System.out.println(result.get("128998").get("发动机转速").size());
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
        Map<String, List<DoublePoint>> result = reader.getDoublePoint(hosts, metrics, 1482319512851L);
        for (Map.Entry<String, List<DoublePoint>> entry : result.entrySet()) {
            System.out.println(entry.getKey());
            for (DoublePoint point : entry.getValue()) {
                System.out.println(point.getMetric() + " " + point.getPrimaryTime()+ " " + point.getValue());
            }
        }
    }
    private static void readLatest(Reader reader) {
        List<String> hosts = new ArrayList<>();
        hosts.add("128280");
        hosts.add("128290");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        Map<String, List<DoublePoint>> result = reader.getDoubleLatest(hosts, metrics);
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
        Map<String, List<DoublePoint>> result = reader.getDoubleRange(hosts, metrics,start.toEpochSecond(TimeUtil.zoneOffset)*1000,end.toEpochSecond(TimeUtil.zoneOffset)*1000);
        for (Map.Entry<String, List<DoublePoint>> entry : result.entrySet()) {
            System.out.println(entry.getKey());
            for (DoublePoint point : entry.getValue()) {
                System.out.println(point.getMetric() + " " + point.getPrimaryTime()+" "+ LocalDateTime.ofEpochSecond(point.getPrimaryTime()/1000,0,TimeUtil.zoneOffset) + " " + point.getValue());
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
            writer.insert("1282835", "APP", start.toEpochSecond(TimeUtil.zoneOffset)*1000, start.toEpochSecond(TimeUtil.zoneOffset)*1000, TimePartition.DAY,value );
            System.out.println("APP" + " " + start.toEpochSecond(TimeUtil.zoneOffset)*1000+" "+ start.toString()+ " " + value);
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

    private static void registerHostTags(Writer writer) {
        Map<String, String> tags = new HashMap<>();
        tags.put("price", "¥.10000");
        writer.registerHostTags("128280", tags);
    }
}
