package com.sagittarius.util;

import com.sagittarius.bean.table.HostMetric;

import java.util.Calendar;

/**
 * Created by qmm on 2016/12/15.
 */
public class TimeUtil {
    public static String getDate(long timeMillis, HostMetric.DateInterval dateInterval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timeMillis);
        switch (dateInterval) {
            case DAY:
                return calendar.get(Calendar.YEAR ) + "D" + calendar.get(Calendar.DAY_OF_YEAR);
            case WEEK:
                return calendar.get(Calendar.YEAR) + "W" + calendar.get(Calendar.WEEK_OF_YEAR);
            case MONTH:
                return calendar.get(Calendar.YEAR) + "M" + (calendar.get(Calendar.MONTH ) + 1);
            case YEAR:
                return calendar.get(Calendar.YEAR) + "";
            default:
                return null;
        }
    }
}
