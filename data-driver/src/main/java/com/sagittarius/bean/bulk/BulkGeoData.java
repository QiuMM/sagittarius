package com.sagittarius.bean.bulk;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.table.GeoData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class BulkGeoData {
    private List<GeoData> datas;

    public BulkGeoData() {
        datas = new ArrayList<>();
    }

    public List<GeoData> getDatas() {
        return datas;
    }

    public void addData(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
        String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
        datas.add(new GeoData(host, metric, timeSlice, primaryTime, secondaryTime, latitude, longitude));
    }
}
