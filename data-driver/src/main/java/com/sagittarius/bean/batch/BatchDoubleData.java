package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.DoubleData;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchDoubleData {
    private List<DoubleData> datas;

    public BatchDoubleData() {
        datas = new ArrayList<>();
    }

    public List<DoubleData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long createdAt, long receivedAt, HostMetric.DateInterval dateInterval, double value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        datas.add(new DoubleData(host, metric, date, createdAt, receivedAt, value));
    }
}
