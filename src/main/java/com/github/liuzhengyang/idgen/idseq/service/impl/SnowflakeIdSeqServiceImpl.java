package com.github.liuzhengyang.idgen.idseq.service.impl;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;
import com.github.liuzhengyang.idgen.idseq.service.impl.snowflake.IdWorker;
import com.github.liuzhengyang.idgen.idseq.service.impl.snowflake.IdWorkerConfigure;

/**
 * @author liuzhengyang
 */
public class SnowflakeIdSeqServiceImpl implements IdSeqService {
    // 获取并锁住唯一的workerId
    private IdWorkerConfigure idWorkerConfigure = new IdWorkerConfigure();
    private IdWorker idWorker;

    public SnowflakeIdSeqServiceImpl(String idType) {
        idWorker = new IdWorker(idWorkerConfigure.getUniqueWorkerId(idType));
    }

    @Override
    public long getId(IdBizType idBizType) {
        return idWorker.nextId();
    }
}
