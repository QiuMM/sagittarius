package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WriteTest {
    private static final Logger logger = LoggerFactory.getLogger(WriteTest.class);
    private static final long DAYINMILLISECOND = 86400000;
    private static File[] fileList = null;
    private int repeat = 0;
    private int fileListIndex = 0;
    private int batchSize;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public WriteTest(String directoryPath, int batchSize) {
        fileList = new File(directoryPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(".xls"))
                    return true;
                return false;
            }
        });
        this.batchSize = batchSize;
    }

    private File getUnProcessedFile() {
        if (fileListIndex < fileList.length)
            return fileList[fileListIndex++];
        else {
            fileListIndex = 0;
            repeat++;
            return fileList[fileListIndex];
        }
    }

    private void test(Writer writer, int concurrencyLevel) {

        List<WriteByXlsTask> tasks = new ArrayList<>();
        for (int i = 0; i < concurrencyLevel; ++i) {
            WriteByXlsTask task = new WriteByXlsTask(writer);
            task.start();
            tasks.add(task);
        }
        while (true) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            double throughput = 0;
            long count = 0;
            for (WriteByXlsTask task : tasks) {
                throughput += task.getThroughput();
                count += task.getCount();
            }
            logger.info("throughput: " + throughput);//+ ", count: " + count);
        }
    }

    public class WriteByXlsTask extends Thread {


        private final Logger logger = LoggerFactory.getLogger(WriteByXlsTask.class);
        private SagittariusWriter writer;
        private String host;
        private long count;
        private double throughput;
        private long consumeTime;
        private int localRepeat;

        public double getThroughput() {
            return throughput;
        }

        public long getCount() {
            return count;
        }

        public void iniate() {
            localRepeat = repeat;
            count = 0;
            throughput = 0;
            consumeTime = 0;
        }

        public WriteByXlsTask(Writer writer) {
            this.writer = (SagittariusWriter)writer;
            count = 0;
            throughput = 0;
            consumeTime = 0;
        }


        public void importSingleFile(File filePath, String fileEncoder, String separtor) {
            InputStream is = null;
            Workbook wb = null;
            host = filePath.getName().split("_")[0];//文件名第一项为device名
            BOMInputStream bomIn ;
            try {
                is = new FileInputStream(filePath);
                bomIn = new BOMInputStream(is, false, ByteOrderMark.UTF_16LE,ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE);
                wb = new HSSFWorkbook(bomIn);
                int offset = 3; //从第4列开始为传感器
                Sheet sheet = wb.getSheetAt(0);
                int rowSize = sheet.getLastRowNum() + 1;
                SagittariusWriter.Data data = writer.newData();
                int batchCount = batchSize;
                double value;
                List<String> metrics = new ArrayList<>();
                Row firstRow = sheet.getRow(0);
                for (int k = 0; k < firstRow.getLastCellNum(); k++) {
                    Cell cell = firstRow.getCell(k);
                    metrics.add(cell.toString());
                }
                for (int j = 1; j < rowSize; j++) {//遍历行
                    Row row = sheet.getRow(j);
                    int cellSize = row.getLastCellNum();//行中有多少个单元格，也就是有多少列
                    long primaryTime = getTimestamp(row.getCell(1).toString()) + DAYINMILLISECOND * localRepeat;
                    long secondaryTime = getTimestamp(row.getCell(2).toString()) + DAYINMILLISECOND * localRepeat;
                    for (int k = offset; k < cellSize; k++) {
                        Cell cell = row.getCell(k);
                        String cellValue;
                        if (cell != null && !(cellValue = cell.toString()).equals("")) {
                            if (batchCount == 0) {
                                try {
                                    long startTime = System.currentTimeMillis();
                                    writer.bulkInsert(data);
                                    consumeTime += System.currentTimeMillis() - startTime;
                                    count += batchSize;
                                    throughput = count / ((double) consumeTime / 1000);
                                }catch (NoHostAvailableException | WriteTimeoutException e) {
                                    //logger.info(e.getMessage());
                                }
                                data = writer.newData();
                                batchCount = batchSize;
                            }
                            try {
                                value = Double.parseDouble(cellValue);
                                data.addDatum(host, metrics.get(k), primaryTime, secondaryTime, TimePartition.DAY, value);
                                batchCount--;
                            } catch (NumberFormatException e) {
                                //logger.warn(e.getMessage());
                            }
                        }
                    }
                }


            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error(e.getMessage());
            } finally {
                if (wb != null) {
                    try {
                        wb.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }

        @Override
        public void run() {
            while (true) {
                iniate();
                File file = getUnProcessedFile();
                importSingleFile(file, "UTF-8", ",");

            }
        }
    }

    public Date getDate(String formatString) {
        Date date = null;
        try {
            date = simpleDateFormat.parse(formatString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public long getTimestamp(String formatString) {

        synchronized (simpleDateFormat) {
            Date date = getDate(formatString);
            return date.getTime();
        }
    }

    public static void main(String[] args) {
        CassandraConnection connection = CassandraConnection.getInstance();
        Cluster cluster = connection.getCluster();
        SagittariusClient client = new SagittariusClient(cluster, new SparkConf(), 10000, 3000, 2);
        Writer writer = client.getWriter();
        new WriteTest(args[0], Integer.parseInt(args[1])).test(writer, Integer.parseInt(args[2]));
        //new WriteTest("./examples/data/201606/", 1000).test(writer, 15);

    }
}
