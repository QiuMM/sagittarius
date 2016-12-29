package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.StringData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkStringData {
    private List<StringData> datas;

    public BulkStringData() {
        datas = new ArrayList<>();
    }

    public List<StringData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new StringData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
