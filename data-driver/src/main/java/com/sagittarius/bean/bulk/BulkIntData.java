package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkIntData {
    private List<IntData> datas;

    public BulkIntData() {
        datas = new ArrayList<>();
    }

    public List<IntData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new IntData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
