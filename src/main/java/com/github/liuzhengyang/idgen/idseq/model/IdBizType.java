package com.github.liuzhengyang.idgen.idseq.model;

/**
 *
 */
public enum IdBizType {
    DEFAULT("default"),
    ORDER_ID("order_id"),
    ;

    private final String bizType;

    IdBizType(String bizType) {
        this.bizType = bizType;
    }

    public String getBizType() {
        return bizType;
    }
}
