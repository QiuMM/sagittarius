package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.bean.table.StringData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchStringData {
    private List<StringData> datas;

    public BatchStringData() {
        datas = new ArrayList<>();
    }

    public List<StringData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long primaryTime, long secondaryTime, HostMetric.DateInterval dateInterval, String value) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        datas.add(new StringData(host, metric, date, primaryTime, secondaryTime, value));
    }
}
