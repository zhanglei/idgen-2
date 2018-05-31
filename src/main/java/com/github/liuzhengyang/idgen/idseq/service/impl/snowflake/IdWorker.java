package com.github.liuzhengyang.idgen.idseq.service.impl.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuzhengyang
 */
public class IdWorker {
    private static final Logger logger = LoggerFactory.getLogger(IdWorker.class);

    // project start time, immutable, to minimize id number
    private static long epoch = 1288834974657L;

    private static final int workerIdBits = 10;
    private static final int maxWorkerId = -1 ^ (-1 << workerIdBits);
    private static final int sequenceBits = 12;
    private static final int workerIdShift = sequenceBits;
    private static final int timestampLeftShift = sequenceBits + workerIdBits;
    private static final int sequenceMask = -1 ^ (-1 << sequenceBits);

    private long lastTimestamp = -1L;

    private final int workerId;

    private long sequence;

    // check workerId

    public IdWorker(int workerId) {
        this.workerId = workerId;
    }

    public synchronized long nextId() {
        long timestamp = timeGen();

        if (timestamp < lastTimestamp) {
            logger.error("Clock is moving backwards. Rejecting requests until {}", lastTimestamp);
            throw new IllegalStateException();
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = timestamp;
        return ((timestamp - epoch) << timestampLeftShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }
}
