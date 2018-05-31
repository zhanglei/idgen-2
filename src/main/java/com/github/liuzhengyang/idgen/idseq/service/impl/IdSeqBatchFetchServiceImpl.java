package com.github.liuzhengyang.idgen.idseq.service.impl;

import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.liuzhengyang.idgen.idseq.model.IdBizType;
import com.github.liuzhengyang.idgen.idseq.service.impl.segment.IdSegment;
import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;

/**
 * @author liuzhengyang
 */
public class IdSeqBatchFetchServiceImpl implements IdSeqService {
    private static final Logger logger = LoggerFactory.getLogger(IdSeqBatchFetchServiceImpl.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final AtomicReference<FutureTask<IdSegment>> currentFetchTask = new AtomicReference<>();
    @GuardedBy("this")
    private volatile AtomicLong currentNextId;
    @GuardedBy("this")
    private volatile IdSegment currentIdSegment;
    private ExecutorService executorService;

    private IdBizType idBizType;

    public IdSeqBatchFetchServiceImpl(IdBizType idBizType) {
        this.idBizType = idBizType;
    }

    // 异步加载segment的默认阈值
    private static final double DEFAULT_ASYNC_FETCH_THRESHOLD_FACTOR = 0.1;

    @PostConstruct
    public void init() {
        executorService = Executors.newSingleThreadExecutor(new CustomizableThreadFactory
                ("Async-Fetch-Segment"));
        DataSource dataSource = jdbcTemplate.getDataSource();
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        currentIdSegment = fetchNewSegment(idBizType);
        currentNextId = new AtomicLong(currentIdSegment.getMaxId() - currentIdSegment.getBatchSize());
    }

    @Override
    public synchronized long getId(IdBizType idBizType) {
        if (currentNextId.get() >= currentIdSegment.getMaxId()) {
            currentIdSegment = fetchNewSegment(idBizType);
            currentNextId = new AtomicLong(currentIdSegment.getMaxId() - currentIdSegment.getBatchSize());
        } else {
            long startId = currentIdSegment.getMaxId() - currentIdSegment.getBatchSize();
            if (currentNextId.get() - startId >= getAsyncFetchThresholdFactor(idBizType) * currentIdSegment.getBatchSize()) {
                if (currentFetchTask.get() == null) {
                    logger.info("Trigger async segment fetch currentId {}, maxId: {}",
                            currentNextId.get(), currentIdSegment.getMaxId());
                    FutureTask<IdSegment> newTask = new FutureTask<>(() -> {
                        return doFetchNewSegment(idBizType);
                    });
                    boolean casSuccess = currentFetchTask.compareAndSet(null, newTask);
                    if (casSuccess) {
                        executorService.submit(newTask);
                    }
                }
            }
        }
        return currentNextId.getAndIncrement();
    }

    private double getAsyncFetchThresholdFactor(IdBizType idBizType) {
        return DEFAULT_ASYNC_FETCH_THRESHOLD_FACTOR;
    }

    private IdSegment fetchNewSegment(IdBizType idBizType) {
        IdSegment result;
        try {
            FutureTask<IdSegment> idSegmentFutureTask = currentFetchTask.get();
            if (idSegmentFutureTask != null) {
                result = idSegmentFutureTask.get();
                currentFetchTask.compareAndSet(idSegmentFutureTask, null);
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
