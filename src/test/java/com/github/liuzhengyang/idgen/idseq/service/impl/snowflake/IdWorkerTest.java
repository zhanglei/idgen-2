package com.github.liuzhengyang.idgen.idseq.service.impl.snowflake;

import static org.junit.Assert.*;

import org.junit.Test;

public class IdWorkerTest {

    @Test
    public void nextId() {
        IdWorker idWorker = new IdWorker(1);
        IdWorker idWorker2 = new IdWorker(2);
        for (int i = 0; i < 1000; i++) {
            System.out.println(idWorker.nextId());
        }
    }
}