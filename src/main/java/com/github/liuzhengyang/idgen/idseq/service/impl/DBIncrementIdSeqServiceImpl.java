package com.github.liuzhengyang.idgen.idseq.service.impl;

import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;

/**
 * CREATE TABLE `id_seq_inc` (
 *   `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * @author liuzhengyang
 */
@Lazy
@Service
public class DBIncrementIdSeqServiceImpl implements IdSeqService {
    private static final String ID_TABLE_NAME = "id_seq_inc";

    @Autowired
    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @PostConstruct
    private void init() {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }


    @Override
    public long getId(IdBizType idBizType) {
        String sql = String.format("insert into %s () values()", ID_TABLE_NAME);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource(), keyHolder);
        return keyHolder.getKey().longValue();
    }
}
