package com.github.liuzhengyang.idgen.idseq.service.impl;

import javax.annotation.Resource;

import org.springframework.data.redis.core.RedisTemplate;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;

/**
 * @author liuzhengyang
 */
public class RedisIdSeqServiceImpl implements IdSeqService {

    @Resource
    private RedisTemplate<String, Long> redisTemplate;

    @Override
    public long getId(IdBizType idBizType) {
        return redisTemplate.opsForValue().increment(idBizType.getBizType(), 1);
    }
}
