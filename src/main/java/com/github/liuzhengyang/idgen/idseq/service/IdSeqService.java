package com.github.liuzhengyang.idgen.idseq.service;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;

/**
 *
 */
public interface IdSeqService {
    default long getId() {
        return getId(IdBizType.DEFAULT);
    }

    long getId(IdBizType idBizType);
}
