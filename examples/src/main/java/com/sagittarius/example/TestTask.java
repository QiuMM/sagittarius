package com.sagittarius.example;

import com.sagittarius.read.SagittariusReader;

import java.io.IOException;

/**
 * Created by qmm on 2017/4/6.
 */
public class TestTask extends Thread {
    private SagittariusReader reader;

    public TestTask(SagittariusReader reader) {
        this.reader = reader;
    }

    @Override
    public void run() {
        try {
            reader.test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
