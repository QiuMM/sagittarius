package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.FloatData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkFloatData {
    private List<FloatData> datas;

    public BulkFloatData() {
        datas = new ArrayList<>();
    }

    public List<FloatData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new FloatData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
