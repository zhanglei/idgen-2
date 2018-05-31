package com.github.liuzhengyang.idgen.idseq.service.impl;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * CREATE TABLE `id_seq_inc` (
 *   `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * @author liuzhengyang
 */
public class DBIncrementIdSeqServiceImpl implements IdSeqService {
    private static final Logger logger = LoggerFactory.getLogger(DBIncrementIdSeqServiceImpl.class);

    private static final String ID_TABLE_NAME = "id_seq_inc";
    private static final int DELETE_BATCH = 10000; // 清理速度

    @Autowired
    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    ScheduledExecutorService scheduledExecutorService;

    @PostConstruct
    private void init() {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("IdDelete"));
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            deleteOldIds();
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    private void destroy() {
        if (scheduledExecutorService != null) {
            MoreExecutors.shutdownAndAwaitTermination(scheduledExecutorService, 1, TimeUnit.HOURS);
        }
    }

    @Override
    public long getId(IdBizType idBizType) {
        String sql = String.format("insert into %s () values()", ID_TABLE_NAME);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource(), keyHolder);
        return keyHolder.getKey().longValue();
    }

    private void deleteOldIds() {
        long maxId = getMaxId();
        String sql = String.format("delete from %s where id < :maxId limit :limit", ID_TABLE_NAME);
        int update = namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource("maxId", maxId)
                .addValue("limit", DELETE_BATCH));
        logger.info("Deleted {} ids", update);
    }

    private long getMaxId() {
        String sql = String.format("select id from %s order by id desc limit 1", ID_TABLE_NAME);
        try {
            return namedParameterJdbcTemplate.queryForObject(sql, new MapSqlParameterSource(), Long.class);
        } catch (EmptyResultDataAccessException e) {
            logger.info("No Id now");
            return 0;
        }
    }
}
