package com.sagittarius.bean.batch;

import com.sagittarius.bean.table.GeoData;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.bean.table.IntData;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmm on 2016/12/17.
 */
public class BatchGeoData {
    private List<GeoData> datas;

    public BatchGeoData() {
        datas = new ArrayList<>();
    }

    public List<GeoData> getDatas() {
        return datas;
    }

    public void add(String host, String metric, long primaryTime, long secondaryTime, HostMetric.DateInterval dateInterval, float latitude, float longitude) {
        String date = TimeUtil.getDate(primaryTime, dateInterval);
        datas.add(new GeoData(host, metric, date, primaryTime, secondaryTime, latitude, longitude));
    }
}
