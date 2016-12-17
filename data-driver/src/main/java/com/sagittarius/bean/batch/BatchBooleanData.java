package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.BooleanData;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchBooleanData {
    private List<BooleanData> datas;

    public BatchBooleanData() {
        datas = new ArrayList<>();
    }

    public List<BooleanData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long createdAt, long receivedAt, HostMetric.DateInterval dateInterval, boolean value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        datas.add(new BooleanData(host, metric, date, createdAt, receivedAt, value));
    }
}
