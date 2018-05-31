package com.github.liuzhengyang.idgen.idseq.service.impl.snowflake;

import java.util.Objects;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuzhengyang
 */
public class IdWorkerConfigure {
    private static final Logger logger = LoggerFactory.getLogger(IdWorkerConfigure.class);
    public int getUniqueWorkerId(String idType) {
        idType = Objects.requireNonNull(idType, "idType");
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("idWorker")
                .build();
        curatorFramework.start();
        int maxRetryCount = 1000;
        int maxWorkerId = 1000;
        int retryCount = 0;
        while (retryCount++ < maxRetryCount) {
            for (int i = 0; i < maxWorkerId; i++) {
                String path = ZKPaths.makePath(idType, String.valueOf(i));
                try {
                    String s = curatorFramework.create()
                            .creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
                    logger.info("Create Path {} Success", s);
                    return i;
                } catch (Exception e) {
                    logger.info("Create Path {} Failed ", path, e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        throw new IllegalStateException();
    }
}
