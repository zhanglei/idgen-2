package com.github.liuzhengyang;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.github.liuzhengyang.idgen.idseq.service.IdSeqService;

/**
 * @author liuzhengyang
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.github.liuzhengyang")
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Resource
    private IdSeqService redisIncrBased;

    @PostConstruct
    public void init() {
        long start = System.currentTimeMillis();
        try {
            pressLoadTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end -start));

    }

    private void pressLoadTest() throws InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(100, 100, 1, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());

        ConcurrentMap<Integer, Integer> verifiedMap = new ConcurrentHashMap<>(10000 * 100);

        for (int i = 0; i < 10000 * 100; i++) {
            int finalI = i;
            executorService.submit(() -> {
                long id = redisIncrBased.getId();
                System.out.println(id);
                verifiedMap.put(finalI, 0);
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(verifiedMap.size());
    }
}
