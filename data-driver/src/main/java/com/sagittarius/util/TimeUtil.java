package com.sagittarius.util;

import com.sagittarius.bean.common.TimePartition;

import java.util.Calendar;

public class TimeUtil {
    public static String generateTimeSlice(long timeMillis, TimePartition timePartition) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timeMillis);
        switch (timePartition) {
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
