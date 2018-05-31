CREATE TABLE `id_seq` (
  `biz_type` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '业务类型',
  `max_id` bigint(20) DEFAULT NULL COMMENT '当前最大id',
  `batch_size` int(11) DEFAULT NULL COMMENT 'id批次取大小',
  `update_time` bigint(20) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`biz_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='批量取id表';

INSERT INTO `id_seq` (`biz_type`, `max_id`, `batch_size`, `update_time`)
VALUES
        ('default',2020000,10000,1527729294389),
        ('order_id',10000,10000,1527729295646);
