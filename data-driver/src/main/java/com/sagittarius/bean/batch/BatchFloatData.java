package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.FloatData;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchFloatData {
    private List<FloatData> datas;

    public BatchFloatData() {
        datas = new ArrayList<>();
    }

    public List<FloatData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long createdAt, long receivedAt, HostMetric.DateInterval dateInterval, float value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        datas.add(new FloatData(host, metric, date, createdAt, receivedAt, value));
    }
}
