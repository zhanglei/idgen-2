package com.github.liuzhengyang.idgen.idseq.service.impl;

import static org.junit.Assert.*;

import org.junit.Test;

public class SnowflakeIdSeqServiceImplTest {

    @Test
    public void getId() throws InterruptedException {
        SnowflakeIdSeqServiceImpl snowflakeIdSeqService = new SnowflakeIdSeqServiceImpl("test");
        for (int i = 0; i < 100000; i++) {
            System.out.println(snowflakeIdSeqService.getId());
            Thread.sleep(100);
        }
    }

    @Test
    public void getId2() throws InterruptedException {
        SnowflakeIdSeqServiceImpl snowflakeIdSeqService = new SnowflakeIdSeqServiceImpl("test");
        for (int i = 0; i < 100000; i++) {
            System.out.println(snowflakeIdSeqService.getId());
            Thread.sleep(100);
        }
    }
}