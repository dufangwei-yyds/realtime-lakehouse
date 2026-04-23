# Flink SQL 作业模板

> 本模板用于快速创建符合规范的 Flink SQL 作业

---

## 模板说明

本模板适用于以下场景：
- ODS 层数据接入
- DIM 层维度构建
- DWD 层明细打宽
- DWS 层主题聚合

---

## 完整作业模板

```sql
/*
============================================================
作业信息
============================================================
作业名称：
作业类型：
数据流向：
创建时间：
创建人：
============================================================
*/

/*
============================================================
步骤 1：环境配置
============================================================
*/
-- 执行模式
SET 'execution.runtime-mode' = 'streaming';

-- 检查点配置（必须）
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

/*
============================================================
步骤 2：Catalog 和数据库
============================================================
*/
CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://192.168.63.128:8020/paimon/erp_dw'
);

USE CATALOG paimon_catalog;
-- CREATE DATABASE IF NOT EXISTS ods;
USE ods;

/*
============================================================
步骤 3：源表定义
============================================================
*/

/* 3.1 Kafka 源表模板 */
-- CREATE TEMPORARY TABLE kafka_source (
--     -- 业务字段
--     xxx_id STRING,
--     xxx_name STRING,
--     xxx_time TIMESTAMP(3),
--     -- Kafka 元数据
--     kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
--     kafka_partition INT METADATA FROM 'partition' VIRTUAL,
--     -- WATERMARK（必须）
--     WATERMARK FOR xxx_time AS xxx_time - INTERVAL '5' MINUTE
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = '${topic_name}',
--     'properties.bootstrap.servers' = '${bootstrap_servers}',
--     'properties.group.id' = 'flink_${layer}_${purpose}_consumer',
--     'scan.startup.mode' = 'latest-offset',
--     'format' = 'json',
--     'json.ignore-parse-errors' = 'true',
--     'json.fail-on-missing-field' = 'false'
-- );

/* 3.2 CDC 源表模板 */
-- CREATE TEMPORARY TABLE cdc_source (
--     xxx_id STRING,
--     xxx_name STRING,
--     xxx_time TIMESTAMP(3),
--     updated_at TIMESTAMP(3),
--     PRIMARY KEY (xxx_id) NOT ENFORCED
-- ) WITH (
--     'connector' = 'mysql-cdc',
--     'hostname' = '${hostname}',
--     'port' = '3306',
--     'username' = '${username}',
--     'password' = '${password}',
--     'database-name' = '${database}',
--     'table-name' = '${table}',
--     'scan.startup.mode' = 'initial'
-- );

/*
============================================================
步骤 4：目标表定义
============================================================
*/

/* 4.1 ODS Append 表模板 */
-- CREATE TABLE IF NOT EXISTS ods_${source}_${table} (
--     -- 业务字段
--     xxx_id STRING,
--     xxx_name STRING,
--     -- 元数据字段
--     _event_time TIMESTAMP(3),
--     _ingest_time TIMESTAMP(3),
--     _partition_dt STRING,
--     _partition_hh STRING,
--     -- Kafka 元数据（Kafka 接入时必须）
--     _kafka_offset BIGINT,
--     _kafka_partition INT,
--     WATERMARK FOR _event_time AS _event_time - INTERVAL '5' SECOND
-- ) PARTITIONED BY (_partition_dt, _partition_hh)
-- WITH (
--     'connector' = 'paimon',
--     'write-mode' = 'append-only',
--     'bucket' = '-1',
--     'file.format' = 'orc'
-- );

/* 4.2 ODS 主键表模板 */
-- CREATE TABLE IF NOT EXISTS ods_${source}_${table} (
--     xxx_id STRING,
--     xxx_name STRING,
--     _event_time TIMESTAMP(3),
--     _ingest_time TIMESTAMP(3),
--     _partition_dt STRING,
--     PRIMARY KEY (xxx_id, _partition_dt) NOT ENFORCED
-- ) PARTITIONED BY (_partition_dt)
-- WITH (
--     'connector' = 'paimon',
--     'write-mode' = 'change-log-upsert',
--     'merge-engine' = 'deduplicate',
--     'bucket' = '4',
--     'sequence.field' = '_ingest_time',
--     'file.format' = 'orc'
-- );

/* 4.3 DIM 宽表模板 */
-- CREATE TABLE IF NOT EXISTS dim_${domain}_wide (
--     xxx_id STRING,
--     xxx_name STRING,
--     _event_time TIMESTAMP(3),
--     _ingest_time TIMESTAMP(3),
--     PRIMARY KEY (xxx_id) NOT ENFORCED
-- ) WITH (
--     'connector' = 'paimon',
--     'write-mode' = 'change-log-upsert',
--     'merge-engine' = 'deduplicate',
--     'bucket' = '4',
--     'file.format' = 'orc'
-- );

/* 4.4 DWD 明细表模板 */
-- CREATE TABLE IF NOT EXISTS dwd_${domain}_${process}_detail_rt (
--     xxx_id STRING,
--     yyy_id STRING,
--     zzz_name STRING,
--     _event_time TIMESTAMP(3),
--     _ingest_time TIMESTAMP(3),
--     _partition_dt STRING,
--     PRIMARY KEY (xxx_id, _partition_dt) NOT ENFORCED
-- ) PARTITIONED BY (_partition_dt)
-- WITH (
--     'connector' = 'paimon',
--     'write-mode' = 'change-log-upsert',
--     'merge-engine' = 'deduplicate',
--     'bucket' = '4',
--     'sequence.field' = '_ingest_time',
--     'file.format' = 'orc'
-- );

/* 4.5 DWS 汇总表模板 */
-- CREATE TABLE IF NOT EXISTS dws_${theme}_${grain}_rt (
--     sku_id STRING,
--     store_id STRING,
--     stat_date STRING,
--     stat_hour STRING,
--     sale_cnt BIGINT,
--     sale_amt DECIMAL(18,2),
--     _event_time TIMESTAMP(3),
--     _ingest_time TIMESTAMP(3),
--     _partition_dt STRING,
--     PRIMARY KEY (sku_id, store_id, stat_date, stat_hour, _partition_dt) NOT ENFORCED
-- ) PARTITIONED BY (_partition_dt)
-- WITH (
--     'connector' = 'paimon',
--     'write-mode' = 'change-log-upsert',
--     'merge-engine' = 'deduplicate',
--     'bucket' = '4',
--     'sequence.field' = '_ingest_time',
--     'file.format' = 'orc'
-- );

/*
============================================================
步骤 5：ETL 逻辑
============================================================
*/

/* 5.1 ODS 摄入逻辑 */
-- INSERT INTO ods_${source}_${table}
-- SELECT
--     -- 业务字段
--     xxx_id,
--     xxx_name,
--     -- 元数据注入
--     xxx_time AS _event_time,
--     CURRENT_TIMESTAMP AS _ingest_time,
--     DATE_FORMAT(xxx_time, 'yyyyMMdd') AS _partition_dt,
--     DATE_FORMAT(xxx_time, 'HH') AS _partition_hh,
--     -- Kafka 元数据
--     kafka_offset AS _kafka_offset,
--     kafka_partition AS _kafka_partition
-- FROM kafka_source
-- WHERE xxx_id IS NOT NULL;

/* 5.2 DIM 构建逻辑 */
-- INSERT INTO dim_${domain}_wide
-- SELECT
--     xxx_id,
--     xxx_name,
--     CURRENT_TIMESTAMP AS _event_time,
--     CURRENT_TIMESTAMP AS _ingest_time
-- FROM ods_${source}_${table};

/* 5.3 DWD 打宽逻辑 */
-- INSERT INTO dwd_${domain}_${process}_detail_rt
-- SELECT
--     o.xxx_id,
--     o.yyy_id,
--     d.zzz_name,
--     o.xxx_time AS _event_time,
--     CURRENT_TIMESTAMP AS _ingest_time,
--     DATE_FORMAT(o.xxx_time, 'yyyyMMdd') AS _partition_dt
-- FROM ods_${source}_${table} o
-- LEFT JOIN dim_${domain}_wide FOR SYSTEM_TIME AS OF o._event_time AS d
-- ON o.xxx_id = d.xxx_id;

/* 5.4 DWS 汇总逻辑 */
-- INSERT INTO dws_${theme}_${grain}_rt
-- SELECT
--     sku_id,
--     store_id,
--     DATE_FORMAT(TUMBLE_START(_event_time, INTERVAL '1' HOUR), 'yyyyMMdd') AS stat_date,
--     DATE_FORMAT(TUMBLE_START(_event_time, INTERVAL '1' HOUR), 'HH') AS stat_hour,
--     COUNT(*) AS sale_cnt,
--     SUM(sale_amount) AS sale_amt,
--     TUMBLE_START(_event_time, INTERVAL '1' HOUR) AS _event_time,
--     CURRENT_TIMESTAMP AS _ingest_time,
--     DATE_FORMAT(TUMBLE_START(_event_time, INTERVAL '1' HOUR), 'yyyyMMdd') AS _partition_dt
-- FROM dwd_${domain}_${process}_detail_rt
-- WHERE order_status = 'SIGNED'
-- GROUP BY
--     TUMBLE(_event_time, INTERVAL '1' HOUR),
--     sku_id,
--     store_id;

/*
============================================================
步骤 6：验证查询
============================================================
*/
-- 验证数据写入
-- SELECT * FROM ods_${source}_${table} WHERE _partition_dt = '${verify_date}' LIMIT 10;

-- 验证数据量
-- SELECT _partition_dt, COUNT(*) AS cnt FROM ods_${source}_${table} GROUP BY _partition_dt ORDER BY _partition_dt DESC LIMIT 7;
```

---

## 使用说明

1. 复制模板到新文件
2. 替换所有 `${xxx}` 占位符
3. 补充业务逻辑
4. 添加验证查询
5. 提交作业

---

## 模板版本

| 版本 | 日期 | 变更说明 |
|-----|------|---------|
| v1.0 | 2026-04 | 初版发布 |
