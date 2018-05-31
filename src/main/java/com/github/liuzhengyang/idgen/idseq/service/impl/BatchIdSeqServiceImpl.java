package com.github.liuzhengyang.idgen.idseq.service.impl;

import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.impl.segment.IdSegment;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;

/**
 * @author liuzhengyang
 */
public class BatchIdSeqServiceImpl implements IdSeqService {
    private static final Logger logger = LoggerFactory.getLogger(BatchIdSeqServiceImpl.class);

    private static final String DEFAULT_BIZ_TYPE = "default";

    private TransactionTemplate transactionTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    // 异步加载segment的默认阈值
    private static final double DEFAULT_ASYNC_FETCH_THRESHOLD_FACTOR = 0.1;

    private final ConcurrentMap<IdBizType, IdSegment> idSegmentMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<IdBizType, AtomicLong> nextIdMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<IdBizType, FutureTask<IdSegment>> idSegmentTaskMap = new
            ConcurrentHashMap<>();

    private final ConcurrentMap<IdBizType, ExecutorService> asyncFetchSegmentExecutorMap = new
            ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        DataSource dataSource = jdbcTemplate.getDataSource();
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public long getId(IdBizType idBizType) {
        synchronized (idBizType) {
            IdSegment idSegment = idSegmentMap.get(idBizType);
            AtomicLong nextId = nextIdMap.get(idBizType);
            if (idSegment == null || nextId == null || nextId.get() >= idSegment.getMaxId()) {
                idSegment = fetchNewSegment(idBizType);
                idSegmentMap.put(idBizType, idSegment);
                nextId = new AtomicLong(idSegment.getMaxId() - idSegment.getBatchSize());
                nextIdMap.put(idBizType, nextId);
            } else {
                long startId = idSegment.getMaxId() - idSegment.getBatchSize();
                if (nextId.get() - startId >= getAsyncFetchThresholdFactor(idBizType) * idSegment.getBatchSize()) {
                    FutureTask<IdSegment> idSegmentFutureTask = getIdSegmentFutureTask(idBizType);
                    if (idSegmentFutureTask == null) {
                        logger.info("Trigger async segment fetch currentId {}, maxId: {}",
                                nextId.get(), idSegment.getMaxId());
                        idSegmentFutureTask = new FutureTask<>(() -> {
                            return doFetchNewSegment(idBizType);
                        });
                        if (idSegmentTaskMap.putIfAbsent(idBizType, idSegmentFutureTask) == null) {
                            getIdSegmentAsyncLoadExecutor(idBizType).submit(idSegmentFutureTask);
                        }
                    }
                }
            }
            return nextId.getAndIncrement();
        }
    }

    @Nullable
    private FutureTask<IdSegment> getIdSegmentFutureTask(IdBizType idBizType) {
        return idSegmentTaskMap.get(idBizType);
    }

    private ExecutorService getIdSegmentAsyncLoadExecutor(IdBizType idBizType) {
        return asyncFetchSegmentExecutorMap.computeIfAbsent(idBizType, type ->
                Executors.newSingleThreadExecutor());
    }

    private double getAsyncFetchThresholdFactor(IdBizType idBizType) {
        return 0.1;
    }

    private IdSegment fetchNewSegment(IdBizType idBizType) {
        IdSegment result;
        try {
            FutureTask<IdSegment> idSegmentFutureTask = getIdSegmentFutureTask(idBizType);
            if (idSegmentFutureTask != null) {
                result = idSegmentFutureTask.get();
                if (getIdSegmentFutureTask(idBizType) != null) {
                    idSegmentTaskMap.remove(idBizType, idSegmentFutureTask);
                }
            } else {
                result = doFetchNewSegment(idBizType);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.info("Async Fetch Failed", e);
            result = doFetchNewSegment(idBizType);
        }
        return result;
    }

    /**
     * 1. lock record lock exclusively
     * 2. update max_id
     * 3. get this segment data
     */
    private IdSegment doFetchNewSegment(IdBizType idBizType) {
        IdSegment idSegment = getTransactionTemplate().execute(new TransactionCallback<IdSegment>() {
            @Override
            public IdSegment doInTransaction(TransactionStatus status) {
                namedParameterJdbcTemplate.update("update id_seq set max_id = max_id + batch_size, update_time = "
                                + ":updateTime where biz_type = :bizType",
                        new MapSqlParameterSource("updateTime", currentTimeMillis())
                                .addValue("bizType", idBizType.getBizType()));
                return namedParameterJdbcTemplate.queryForObject("select * from id_seq where biz_type"
                                + " = :bizType",
                        new MapSqlParameterSource("bizType", idBizType.getBizType()),
                        new BeanPropertyRowMapper<>(IdSegment.class));
            }
        });
        logger.info("Fetch new segment result: {}", idSegment);
        return idSegment;
    }

    private TransactionTemplate getTransactionTemplate() {
        DataSource dataSource = jdbcTemplate.getDataSource();
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }
}
