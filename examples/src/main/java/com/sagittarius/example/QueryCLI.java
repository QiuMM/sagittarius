//package com.sagittarius.example;
//
//import com.datastax.driver.core.Cluster;
//import com.sagittarius.bean.query.Shift;
//import com.sagittarius.bean.result.*;
//import com.sagittarius.core.SagittariusClient;
//import com.sagittarius.read.Reader;
//import org.apache.commons.cli.*;
//import org.apache.spark.SparkConf;
//
//import java.time.LocalDateTime;
//import java.time.ZoneOffset;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeParseException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.Scanner;
//import java.util.regex.PatternSyntaxException;
//
//public class QueryCLI {
//    static final String OUTPUTFORMAT="  %s\t\t%s\t\t\t%d\t\t";
//    public static void main(String[] args) {
//        Scanner sc = new Scanner(System.in);
//        CassandraConnection connection = CassandraConnection.getInstance();
//        Cluster cluster = connection.getCluster();
//        SagittariusClient client = new SagittariusClient(cluster, new SparkConf(), 10000, 3000, 2);
//        Reader reader = client.getReader();
//        System.out.println("-------------------------------------------------------------\nWelcome to");
//        String logo = "            __ __ __  ____  __ ______     \n" +
//                "           / //_//  |/  / |/ //_  __/_  __\n" +
//                "          / ,<  / /|_/ /|   /  / / / / / /\n" +
//                "         / /| |/ /  / //   |_ / / / /_/ / \n" +
//                "        /_/ |_/_/  /_//_/|_(_)_/  \\__, /  version 0.1\n" +
//                "                                 /____/   ";
//        System.out.println(logo + "\n\n-------------------------------------------------------------");
//        System.out.println("Use long2date to transform a long to a date");
//        System.out.println("Use date2long to transform a date to a long");
//        System.out.print("query>");
//        while (sc.hasNext()) {
//            String cmdString = sc.nextLine();
//            if (cmdString.equals("")) {
//                continue;
//            }
//            if(cmdString.startsWith("date2long"))
//            {
//                String[] tmp= cmdString.split(" ");
//                if(tmp.length==1)
//                    System.out.println("Note: you should type a date behind command date2long");
//                else {
//                    try {
//                        date2Long(tmp[1]);
//                    }
//                    catch (DateTimeParseException e){
//                        System.out.println("the Date should be in the form of \"2011-12-03T10:15:30.000\" ");
//                    }
//                }
//            }
//            else if(cmdString.startsWith("long2date"))
//            {
//                String[] tmp= cmdString.split(" ");
//                if(tmp.length==1)
//                    System.out.println("Note: you should type a long behind command long2date");
//                else
//                    long2Date(Long.parseLong(tmp[1]));
//            }
//            else {//if(cmdString.contains("get")){
//                cmd(cmdString.split(" "), reader);
//            }
//            System.out.print("query>");
//        }
//    }
//    private static void long2Date(long time){
//        System.out.println(LocalDateTime.ofEpochSecond(time/1000,(int)(time%1000)*1000000, ZoneOffset.UTC).toString());
//    }
//    private static void date2Long(String date)throws DateTimeParseException {
//        LocalDateTime localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
//        System.out.println(localDateTime.toEpochSecond(ZoneOffset.UTC)*1000+localDateTime.getNano()/1000000);
//    }
//
//    private static void cmd(String[] args, Reader reader) {
//        Options options = initOptions();
//        HelpFormatter hf = new HelpFormatter();
//        hf.setWidth(110);
//        CommandLine commandLine;
//        CommandLineParser parser = new DefaultParser();
//        try {
//            commandLine = parser.parse(options, args);
//            if (commandLine.hasOption('h')) {
//                hf.printHelp("get", options, true);
//                return;
//            } else {
//                String hostString = commandLine.getOptionValue('d');
//                String metricString = commandLine.getOptionValue('s');
//                String dataType = commandLine.getOptionValue('t');
//                List<String> hosts, metrics;
//                System.out.println("   host   |   metric   |      primary_time    |      value");
//                System.out.println("----------+------------+----------------------+------------------");
//                try {
//                    hosts = stringToList(hostString);
//                    metrics = stringToList(metricString);
//                } catch (PatternSyntaxException e) {
//                    System.out.println("The list is in illegal form, must be element1,element2,……,elementN");
//                    return;
//                }
//                if (commandLine.hasOption('l')) {
//                    readLatest(hosts, metrics, dataType, reader);
//                } else if (commandLine.hasOption('p')) {
//                    long time = Long.parseLong(commandLine.getOptionValue('p'));
//                    readPoint(hosts, metrics, time, dataType, reader);
//                } else if (commandLine.hasOption('r')) {
//                    String[] values = commandLine.getOptionValues('r');
//                    long start = Long.parseLong(values[0]);
//                    long end = Long.parseLong(values[1]);
//                    readbyRange(hosts, metrics, start, end, dataType, reader);
//                } else if (commandLine.hasOption('f')) {
//                    String[] values = commandLine.getOptionValues('f');
//                    String shift = values[0];
//                    long time = Long.parseLong(values[1]);
//                    if (hosts.size() > 1 || metrics.size() > 1) {
//                        System.out.println("Fuzzy query only support one host and one metric.");
//                        return;
//                    }
//                    readFuzzy(hosts.get(0), metrics.get(0), stringToShift(shift), time, dataType, reader);
//                }
//            }
//
//
//        } catch (ParseException e) {
//            hf.printHelp("get", options, true);
//        }
//    }
//
//    private static Options initOptions() {
//        Option help = new Option("h", "the command help");
//
//        Option host = Option.builder("d").longOpt("devices").hasArg().argName("devices list").desc(
//                "List of devices,separated by comma ',' ").required().build();
//        Option metric = Option.builder("s").longOpt("sensors").hasArg().argName("sensors list").desc(
//                "List of sensors,separated by comma',' ").required().build();
//        Option dataType = Option.builder("t").longOpt("type").hasArg().argName("data type").desc(
//                "Type of points, can be INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, GEO").required().build();
//        Option point = Option.builder("p").longOpt("point").hasArg().argName("query time").desc(
//                "If specified, will search points at the given query time").build();
//        Option fuzzy = Option.builder("f").longOpt("fuzzy").argName("shift> <query time").hasArg().numberOfArgs(2).desc(
//                "If specified, will search a point according to the shift option, the option can be BEFORE, AFTER or NEAREST, " +
//                        "but this query can only be applied to only one metric related to a certain host ").build();
//        Option latest = Option.builder("l").longOpt("latest").desc(
//                "If set, will search the latest points").build();
//        Option range = Option.builder("r").longOpt("range").hasArg().argName("start time> <end time").numberOfArgs(2).desc(
//                "If specified, will search points in the given time range.").build();
//        OptionGroup optionGroup = new OptionGroup();
//        optionGroup.addOption(point);
//        optionGroup.addOption(fuzzy);
//        optionGroup.addOption(latest);
//        optionGroup.addOption(range);
//        optionGroup.setRequired(true);
//        Options opts = new Options();
//        opts.addOption(host);
//        opts.addOption(metric);
//        opts.addOption(dataType);
//        opts.addOptionGroup(optionGroup);
//        opts.addOption(help);
//        return opts;
//    }
//
//    private static List<String> stringToList(String str) throws PatternSyntaxException {
//        String[] list = str.split(",", -1);
//        return Arrays.asList(list);
//    }
//
//    private static Shift stringToShift(String shift) {
//        switch (shift) {
//            case "AFTER":
//                return Shift.AFTER;
//            case "BEFORE":
//                return Shift.BEFORE;
//            default:
//                return Shift.NEAREST;
//        }
//    }
//
//    private static void readPoint(List<String> hosts, List<String> metrics, long time, String type, Reader reader) {
//        switch (type) {
//            case "INT":
//                Map<String, Map<String, List<IntPoint>>> resultInt = reader.getIntPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<IntPoint>>> entry : resultInt.entrySet()){
//                    for(Map.Entry<String, List<IntPoint>> e : entry.getValue().entrySet()){
//                        for(IntPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "LONG":
//                Map<String, Map<String, List<LongPoint>>> resultLong = reader.getLongPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<LongPoint>>> entry : resultLong.entrySet()){
//                    for(Map.Entry<String, List<LongPoint>> e : entry.getValue().entrySet()){
//                        for(LongPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "FLOAT":
//                Map<String, Map<String, List<FloatPoint>>> resultFloat = reader.getFloatPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<FloatPoint>>> entry : resultFloat.entrySet()){
//                    for(Map.Entry<String, List<FloatPoint>> e : entry.getValue().entrySet()){
//                        for(FloatPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "DOUBLE":
//                Map<String, Map<String, List<DoublePoint>>> resultDouble = reader.getDoublePoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<DoublePoint>>> entry : resultDouble.entrySet()){
//                    for(Map.Entry<String, List<DoublePoint>> e : entry.getValue().entrySet()){
//                        for(DoublePoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "BOOLEAN":
//                Map<String, Map<String, List<BooleanPoint>>> resultBoolean = reader.getBooleanPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<BooleanPoint>>> entry : resultBoolean.entrySet()){
//                    for(Map.Entry<String, List<BooleanPoint>> e : entry.getValue().entrySet()){
//                        for(BooleanPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "STRING":
//                Map<String, Map<String, List<StringPoint>>> resultString = reader.getStringPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<StringPoint>>> entry : resultString.entrySet()){
//                    for(Map.Entry<String, List<StringPoint>> e : entry.getValue().entrySet()){
//                        for(StringPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "GEO":
//                Map<String, Map<String, List<GeoPoint>>> resultBoolean = reader.getBooleanPoint(hosts, metrics, time);
//                for(Map.Entry<String, Map<String, List<BooleanPoint>>> entry : resultBoolean.entrySet()){
//                    for(Map.Entry<String, List<BooleanPoint>> e : entry.getValue().entrySet()){
//                        for(BooleanPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//        }
//    }
//
//    private static void readLatest(List<String> hosts, List<String> metrics, String type, Reader reader) {
//        switch (type) {
//            case "INT":
//                Map<String, Map<String, List<IntPoint>>> result = reader.getIntLatest(hosts, metrics);
//                for(Map.Entry<String, Map<String, List<IntPoint>>> entry : result.entrySet()){
//                    for(Map.Entry<String, List<IntPoint>> e : entry.getValue().entrySet()){
//                        for(IntPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "LONG":
//                for (Map.Entry<String, List<LongPoint>> entry : reader.getLongLatest(hosts, metrics).entrySet()) {
//
//                    for (LongPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "FLOAT":
//                for (Map.Entry<String, List<FloatPoint>> entry : reader.getFloatLatest(hosts, metrics).entrySet()) {
//
//                    for (FloatPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "DOUBLE":
//                for (Map.Entry<String, List<DoublePoint>> entry : reader.getDoubleLatest(hosts, metrics).entrySet()) {
//
//                    for (DoublePoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "BOOLEAN":
//                for (Map.Entry<String, List<BooleanPoint>> entry : reader.getBooleanLatest(hosts, metrics).entrySet()) {
//
//                    for (BooleanPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "STRING":
//                for (Map.Entry<String, List<StringPoint>> entry : reader.getStringLatest(hosts, metrics).entrySet()) {
//
//                    for (StringPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "GEO":
//                for (Map.Entry<String, List<GeoPoint>> entry : reader.getGeoLatest(hosts, metrics).entrySet()) {
//
//                    for (GeoPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + "("+point.getLatitude() + "," + point.getLongitude()+")");
//                    }
//                }
//                break;
//        }
//
//    }
//
//    private static void readbyRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String type, Reader reader) {
//        switch (type) {
//            case "INT":
//                Map<String, Map<String, List<IntPoint>>> result = reader.getIntRange(hosts, metrics, startTime, endTime);
//                for(Map.Entry<String, Map<String, List<IntPoint>>> entry : result.entrySet()){
//                    for(Map.Entry<String, List<IntPoint>> e : entry.getValue().entrySet()){
//                        for(IntPoint point : e.getValue()){
//                            System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                        }
//                    }
//                }
//                break;
//            case "LONG":
//                for (Map.Entry<String, List<LongPoint>> entry : reader.getLongRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (LongPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "FLOAT":
//                for (Map.Entry<String, List<FloatPoint>> entry : reader.getFloatRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (FloatPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "DOUBLE":
//                for (Map.Entry<String, List<DoublePoint>> entry : reader.getDoubleRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (DoublePoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "BOOLEAN":
//                for (Map.Entry<String, List<BooleanPoint>> entry : reader.getBooleanRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (BooleanPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "STRING":
//                for (Map.Entry<String, List<StringPoint>> entry : reader.getStringRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (StringPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + point.getValue());
//                    }
//                }
//                break;
//            case "GEO":
//                for (Map.Entry<String, List<GeoPoint>> entry : reader.getGeoRange(hosts, metrics, startTime, endTime).entrySet()) {
//
//                    for (GeoPoint point : entry.getValue()) {
//                        System.out.println(String.format(OUTPUTFORMAT,entry.getKey(),point.getMetric(),point.getPrimaryTime()) + "("+point.getLatitude() + "," + point.getLongitude()+")");
//                    }
//                }
//                break;
//        }
//    }
//
//    private static void readFuzzy(String host, String metric, Shift shift, long time, String type, Reader reader) {
//
//        switch (type) {
//            case "INT":
//                IntPoint pointInt = reader.getFuzzyIntPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointInt.getPrimaryTime()) + pointInt.getValue());
//                break;
//            case "LONG":
//                LongPoint pointLong = reader.getFuzzyLongPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointLong.getPrimaryTime())+ pointLong.getValue());
//                break;
//            case "FLOAT":
//                FloatPoint pointFloat = reader.getFuzzyFloatPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointFloat.getPrimaryTime()) + pointFloat.getValue());
//                break;
//            case "DOUBLE":
//                DoublePoint pointDouble = reader.getFuzzyDoublePoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointDouble.getPrimaryTime()) + pointDouble.getValue());
//                break;
//            case "BOOLEAN":
//                BooleanPoint pointBoolean = reader.getFuzzyBooleanPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointBoolean.getPrimaryTime()) + pointBoolean.getValue());
//                break;
//            case "STRING":
//                StringPoint pointString = reader.getFuzzyStringPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointString.getPrimaryTime()) + pointString.getValue());
//                break;
//            case "GEO":
//                GeoPoint pointGeo = reader.getFuzzyGeoPoint(host, metric, time, shift);
//                System.out.println(String.format(OUTPUTFORMAT,host,metric,pointGeo.getPrimaryTime())+"("+ pointGeo.getLatitude() + "," + pointGeo.getLongitude()+")");
//
//                break;
//        }
//
//    }
//
//
//}
