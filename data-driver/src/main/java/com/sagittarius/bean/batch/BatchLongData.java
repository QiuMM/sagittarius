package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.bean.table.LongData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchLongData {
    private List<LongData> datas;

    public BatchLongData() {
        datas = new ArrayList<>();
    }

    public List<LongData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long createdAt, long receivedAt, HostMetric.DateInterval dateInterval, long value) {
        String date = TimeUtil.getDate(receivedAt, dateInterval);
        datas.add(new LongData(host, metric, date, createdAt, receivedAt, value));
    }
}
