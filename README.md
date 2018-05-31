# Unique Id Generator

## Implementations
1. batch segment id [batch-id](https://github.com/liuzhengyang/idgen/blob/master/src/main/java/com/github/liuzhengyang/idgen/idseq/service/impl/IdSeqBatchFetchServiceImpl.java)
2. snowflake [snowflake](https://github.com/liuzhengyang/idgen/blob/master/src/main/java/com/github/liuzhengyang/idgen/idseq/service/impl/SnowflakeIdSeqServiceImpl.java)
3. mysql autoincrement primary key [auto-increment-key](https://github.com/liuzhengyang/idgen/blob/master/src/main/java/com/github/liuzhengyang/idgen/idseq/service/impl/DBIncrementIdSeqServiceImpl.java)
4. redis incr [redis](https://github.com/liuzhengyang/idgen/blob/master/src/main/java/com/github/liuzhengyang/idgen/idseq/service/impl/RedisIdSeqServiceImpl.java)

## Pros and Cons