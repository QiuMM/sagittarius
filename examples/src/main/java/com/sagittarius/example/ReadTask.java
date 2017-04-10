package com.sagittarius.example;

import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.read.Reader;
import com.sagittarius.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by qmm on 2017/3/27.
 */
public class ReadTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(ReadTask.class);

    private Reader reader;
    private long start;
    private String filter;

    public ReadTask(Reader reader, long start, String filter) {
        this.reader = reader;
        this.start = start;
        this.filter = filter;
    }

    @Override
    public void run() {
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128998");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("发动机转速");
        long start = LocalDateTime.of(2017,2,26,0,0).toEpochSecond(TimeUtil.zoneOffset)*1000;
        long end = LocalDateTime.of(2017,2,27,23,59).toEpochSecond(TimeUtil.zoneOffset)*1000;

        Map<String, Map<String, List<FloatPoint>>> result = reader.getFloatRange(hosts, metrics, start, end, this.filter);
        System.out.println(result.get("128998").get("发动机转速").size());
        logger.info("consume time: " + (System.currentTimeMillis() - this.start) + "ms");
    }
}
