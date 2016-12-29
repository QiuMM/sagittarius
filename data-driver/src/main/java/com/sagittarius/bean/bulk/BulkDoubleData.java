package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.DoubleData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkDoubleData {
    private List<DoubleData> datas;

    public BulkDoubleData() {
        datas = new ArrayList<>();
    }

    public List<DoubleData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new DoubleData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
