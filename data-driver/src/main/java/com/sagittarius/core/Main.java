package com.sagittarius.core;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by qmm on 2016/12/16.
 */
public class Main {
    public static void main(String[] args) {
        test(new String("qmm"));
    }

    private static void test(Object value) {

        List<String> aa = new ArrayList<String>();
        aa.add("q");
        aa.add("m");
        String str = String.format(("qmm %d"), 1481973091385L);
        System.out.println(System.currentTimeMillis());
        System.out.println(str);
    }
}
