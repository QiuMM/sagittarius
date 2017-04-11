package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.SparkException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.read.Reader;
import com.sagittarius.read.SagittariusReader;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.Writer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;


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
        sparkConf.set("spark.cassandra.connection.keep_alive_ms", "600000");
        //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        //sparkConf.set("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/agittarius/");
        //sparkConf.set("spark.scheduler.mode", "FAIR");
        //sparkConf.set("spark.executor.cores", "4");
        sparkConf.set("spark.cores.max", "20");
        //sparkConf.set("spark.driver.maxResultSize", "20g");
        //sparkConf.set("spark.driver.memory", "20g");
        sparkConf.set("spark.executor.memory", "2g");
        SagittariusClient client = new SagittariusClient(cluster, sparkConf, 10000);
        Writer writer = client.getWriter();
        SagittariusReader reader = (SagittariusReader)client.getReader();
        ReadTask task1 = new ReadTask(reader, time, "value >= 33 and value <= 34");
        ReadTask task2 = new ReadTask(reader, time, "value >= 34 and value <= 35");
        ReadTask task3 = new ReadTask(reader, time, "value >= 35 and value <= 36");
        //batchTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //batchTest1(client, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //insert(writer);
        //task1.start();
        //task2.start();
        //task3.start();
        //test(client.getSparkContext());
        //floatRead(reader);
        //insert(writer);
        TestTask testTask = new TestTask(reader);
        testTask.start();
        //floatRead(reader);
        //logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
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

    public static void test(JavaSparkContext sparkContext) {
        long time = System.currentTimeMillis();
        /*Map<String, String> map = new HashMap<>();
        map.put("keyspace", "sagittarius");
        map.put("table", "data_float");
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> dataset = sqlContext.read().format("org.apache.spark.sql.cassandra").options(map).load().filter("value >= 33 and value <= 34");

        //dataset.filter(dataset.apply("value").$greater(33));
        //dataset.apply("").
        //dataset = dataset.filter("value >= 33 and value <= 34");
        //dataset = dataset.selectExpr("host");
        dataset.explain();
        System.out.println(dataset.count());

        System.out.println("consume time :" + (System.currentTimeMillis() - time));*/
        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float");
        //JavaRDD<CassandraRow> rdd1 = rdd.filter(r -> r.getFloat("value") >= 33 && r.getFloat("value") <= 34);
        rdd.collect();

        System.out.println("consume time :" + (System.currentTimeMillis() - time) + " ");
    }

    private static void floatRead(Reader reader){
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128998");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("发动机转速");
        long start = LocalDateTime.of(2017,2,20,0,0).toEpochSecond(TimeUtil.zoneOffset)*1000;
        long end = LocalDateTime.of(2017,2,27,23,59).toEpochSecond(TimeUtil.zoneOffset)*1000;
        String filter = "value >= 33 and value <= 34";
        Map<String, Map<String, List<FloatPoint>>> result = null;
        try {
            result = reader.getFloatRange(hosts, metrics, start, end, filter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(result.get("128998").get("发动机转速").size());
    }

    private static void batchTest1(SagittariusClient client, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchTest> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchTest task = new BatchTest(client.getWriter(), host, runTime);
            task.start();
            tasks.add(task);
        }

        long start = System.currentTimeMillis();
        long count = 0;
        long consume;
        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (BatchTest task : tasks) {
                count += task.getCount();
            }
            consume = System.currentTimeMillis() - start;
            double throughput = count / ((double) consume / 1000);

            logger.info("throughput: " + throughput + ", count: " + count);
            count = 0;
        }
    }

    private static void batchTest(Writer writer, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchTest> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchTest task = new BatchTest(writer, host, runTime);
            task.start();
            tasks.add(task);
        }

        long start = System.currentTimeMillis();
        long count = 0;
        long consume;
        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (BatchTest task : tasks) {
                count += task.getCount();
            }
            consume = System.currentTimeMillis() - start;
            double throughput = count / ((double) consume / 1000);
            count = 0;
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
        Map<String, Map<String, DoublePoint>> result = null;
        try {
            result = reader.getDoublePoint(hosts, metrics, 1482319512851L);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, DoublePoint>> entry : result.entrySet()) {
            //
        }
    }
    private static void readLatest(Reader reader) {
        List<String> hosts = new ArrayList<>();
        hosts.add("128280");
        hosts.add("128290");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        Map<String, Map<String, DoublePoint>> result = null;
        try {
            result = reader.getDoubleLatest(hosts, metrics);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, DoublePoint>> entry : result.entrySet()) {
            //
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
        Map<String, Map<String, List<DoublePoint>>> result = null;
        try {
            result = reader.getDoubleRange(hosts, metrics,start.toEpochSecond(TimeUtil.zoneOffset)*1000,end.toEpochSecond(TimeUtil.zoneOffset)*1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, List<DoublePoint>>> entry : result.entrySet()) {
            //
        }
    }

    private static void readFuzzy(Reader reader) {

        String host="128280";
        String metric="APP";
        DoublePoint point = null;
        try {
            point = reader.getFuzzyDoublePoint(host,metric,1483712410000L, Shift.NEAREST);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(point.getMetric() + " " + point.getPrimaryTime() + " " + point.getValue());

    }

    private static void insertLoop(Writer writer) {
        LocalDateTime start = LocalDateTime.of(1993,10,12,0,0);
        LocalDateTime end = LocalDateTime.of(1993,10,14,23,59);
        System.out.println("插入");
        while (!start.isAfter(end)) {
            double value=Math.random()*100;
            try {
                writer.insert("1282835", "APP", start.toEpochSecond(TimeUtil.zoneOffset)*1000, start.toEpochSecond(TimeUtil.zoneOffset)*1000, TimePartition.DAY,value );
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("APP" + " " + start.toEpochSecond(TimeUtil.zoneOffset)*1000+" "+ start.toString()+ " " + value);
            start = start.plusHours(6);
        }
    }

    private static void insert(Writer writer) {
        long time1 = System.currentTimeMillis();
        long time2 = System.currentTimeMillis();
        System.out.println(time1);
        System.out.println(time2);
        //for (int i = 0; i < 3000; ++i) {
        try {
            writer.insert("128280", "APP", time1, -1, TimePartition.DAY, 10l);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ++time1;
        //}
        logger.info("" + (System.currentTimeMillis() - time1));
        /*long start = System.currentTimeMillis();
        SagittariusWriter sWriter = (SagittariusWriter)writer;
        SagittariusWriter.Datas datas = sWriter.newDatas();
        long time = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < 3000; ++i) {
            datas.addData("128280", "APP", time, time, TimePartition.DAY, random.nextDouble() * 100);
            ++time;
        }
        sWriter.bulkInsert(datas);
        logger.info(" " + (System.currentTimeMillis() - start));*/
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
            try {
                writer.registerHostMetricInfo(host, metricMetadatas);
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
        }
    }

    private static void registerHostTags(Writer writer) {
        Map<String, String> tags = new HashMap<>();
        tags.put("price", "¥.10000");
        try {
            writer.registerHostTags("128280", tags);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
