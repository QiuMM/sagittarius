package com.sagittarius.write.internals;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handles the sending of records to the server
 */
public class Sender implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private Session session;
    private final RecordAccumulator accumulator;
    public Sender(Session session, RecordAccumulator accumulator) {
        this.session = session;
        this.accumulator = accumulator;
    }

    @Override
    public void run() {
        while (true) {
            try {
                run(System.currentTimeMillis());
            } catch (Exception e) {
                logger.error("Uncaught error in data send thread: ", e);
            }
        }
    }

    private void run(long nowMs) {
        RecordBatch batch = accumulator.getReadyBatch(nowMs);
        if (batch != null) {
            try {
                int records = batch.batchStatement.size();
                session.execute(batch.sendableStatement());
                logger.debug("Insert {} records to database", records);
            } catch (Exception e) {

            }
        }
    }
}
