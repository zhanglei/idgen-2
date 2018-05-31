package com.github.liuzhengyang.idgen.idseq.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.impl.BatchIdSeqServiceImpl;
import com.github.liuzhengyang.idgen.idseq.service.impl.DBIncrementIdSeqServiceImpl;
import com.github.liuzhengyang.idgen.idseq.service.impl.IdSeqBatchFetchServiceImpl;
import com.github.liuzhengyang.idgen.idseq.service.impl.RedisIdSeqServiceImpl;
import com.github.liuzhengyang.idgen.idseq.service.impl.SnowflakeIdSeqServiceImpl;

/**
 * @author liuzhengyang
 */
@Configuration
public class IdSeqFactory {

    @Bean
    public IdSeqService snowflakeIdSeq() {
        return new SnowflakeIdSeqServiceImpl("snowflake");
    }

    @Bean
    public IdSeqService batchIdSingleBased() {
        return new IdSeqBatchFetchServiceImpl(IdBizType.ORDER_ID);
    }

    @Bean
    public IdSeqService batchIdBased() {
        return new BatchIdSeqServiceImpl();
    }

    @Bean
    public IdSeqService mysqlIncrBased() {
        return new DBIncrementIdSeqServiceImpl();
    }

    @Bean
    public IdSeqService redisIncrBased() {
        return new RedisIdSeqServiceImpl();
    }
}
