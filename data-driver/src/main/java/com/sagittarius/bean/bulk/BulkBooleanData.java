package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.BooleanData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkBooleanData {
    private List<BooleanData> datas;

    public BulkBooleanData() {
        datas = new ArrayList<>();
    }

    public List<BooleanData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new BooleanData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
