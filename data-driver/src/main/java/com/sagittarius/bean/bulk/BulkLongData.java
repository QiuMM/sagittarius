package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.LongData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkLongData {
    private List<LongData> datas;

    public BulkLongData() {
        datas = new ArrayList<>();
    }

    public List<LongData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new LongData(host, metric, timeSlice, primaryTime, secondaryTime, value));
    }
}
