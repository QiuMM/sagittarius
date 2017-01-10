package com.sagittarius.util;

import com.sagittarius.bean.common.TimePartition;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;

public class TimeUtil {
    /*public static String generateTimeSlice(long timeMillis, TimePartition timePartition) {
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
    }*/
    public static String generateTimeSlice(long timeMillis, TimePartition timePartition) {
        LocalDateTime time = LocalDateTime.ofEpochSecond(timeMillis/1000, 0, ZoneOffset.UTC);
        switch (timePartition) {
            case DAY:
                return time.getYear() + "D" + time.getDayOfYear();
            case WEEK:
                return time.getYear() + "W" + time.get(ALIGNED_WEEK_OF_YEAR);
            case MONTH:
                return time.getYear() + "M" + time.getMonthValue();
            case YEAR:
                return time.getYear() + "";
            default:
                return null;
        }
    }

    public static String getYearFromTimeSlice(String timeSlice, TimePartition timePartition){
        switch (timePartition) {
            case DAY:
                return timeSlice.substring(0,timeSlice.indexOf("D") + 1);
            case WEEK:
                return timeSlice.substring(0,timeSlice.indexOf("W") + 1);
            case MONTH:
                return timeSlice.substring(0,timeSlice.indexOf("M") + 1);
            case YEAR:
                return timeSlice;
            default:
                return null;
        }

    }
    public static boolean ifInSameYear(String timeSlice1,String timeSlice2, TimePartition timePartition){
        if(getYearFromTimeSlice(timeSlice1,timePartition).equals(getYearFromTimeSlice(timeSlice2,timePartition)))
            return true;
        return false;

    }
}
