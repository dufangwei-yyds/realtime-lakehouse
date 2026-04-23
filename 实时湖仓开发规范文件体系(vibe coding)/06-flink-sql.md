# Flink SQL 开发规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Flink SQL 作业开发规范、作业模板、代码组织  
> 适用范围：ODS/DIM/DWD/DWS 全链路 Flink SQL 开发、Flink 作业编写

---

## 一、Flink SQL 适用场景

### 1.1 推荐使用场景

| 场景 | 适用原因 |
|-----|---------|
| ODS 标准化接入 | Flink SQL 声明式，易于维护 |
| DIM 宽表构建 | JOIN 语法直观，维度关联清晰 |
| DWD 明细打宽 | 一表多 JOIN，SQL 可读性好 |
| DWS 主题聚合 | 窗口聚合函数支持完善 |
| 实时 ETL 清洗 | 数据转换逻辑清晰 |

### 1.2 不适用场景

? **以下场景建议使用 DataStream API**

| 场景 | 原因 |
|-----|------|
| ODS 维度 SCD2 处理 | 需要强状态控制 |
| 复杂状态管理 | 需要自定义状态后端 |
| 复杂事件处理 | 需要 ProcessFunction |
| 非结构化数据处理 | 需要自定义解析逻辑 |

---

## 二、作业结构规范

### 2.1 Flink SQL 作业模板

```sql
/*
============================================================
作业说明：ODS 层 APP 点击流清洗与摄入
作业类型：ODS 层接入
数据流向：Kafka -> Paimon ODS 表
创建时间：2026-04-13
创建人：Claude Code AI
============================================================
*/

/*
============================================================
步骤 1：环境配置
============================================================
*/
-- 设置执行模式
SET 'execution.runtime-mode' = 'streaming';

-- 设置检查点间隔（必须配置）
SET 'execution.checkpointing.interval' = '10s';

-- 设置检查点超时
SET 'execution.checkpointing.timeout' = '10min';

-- 设置状态后端（可选，建议生产环境使用 rocksdb）
-- SET 'state.backend' = 'rocksdb';

/*
============================================================
步骤 2：Catalog 和数据库创建
============================================================
*/
CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://192.168.63.128:8020/paimon/erp_dw'
);

USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

/*
============================================================
步骤 3：定义源表（Kafka / CDC）
============================================================
*/
-- 3.1 Kafka 源表定义
CREATE TEMPORARY TABLE kafka_app_click_source (
    -- 业务核心字段
    uid STRING,
    item_id STRING,
    action STRING,
    event_ts TIMESTAMP(3),
    ext_info STRING,
    -- Kafka 元数据字段
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    -- WATERMARK 声明（必须）
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'fashion_app_click',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink_ods_app_click_consumer',
    'scan.startup.mode' = 'latest-offset',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

/*
============================================================
步骤 4：定义目标表（Paimon ODS 表）
============================================================
*/
-- 4.1 ODS 原始事实表（Append 模式）
CREATE TABLE IF NOT EXISTS ods_kafka_app_click (
    -- 业务核心字段
    user_id STRING,
    sku_id STRING,
    behavior STRING,
    raw_json STRING,
    -- 元数据字段
    _event_time TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _partition_dt STRING,
    _partition_hh STRING,
    -- Kafka 元数据
    _kafka_offset BIGINT,
    _kafka_partition INT,
    WATERMARK FOR _event_time AS _event_time - INTERVAL '5' SECOND
) PARTITIONED BY (_partition_dt, _partition_hh)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'append-only',
    'bucket' = '-1',
    'file.format' = 'orc'
);

/*
============================================================
步骤 5：ETL 逻辑编写
============================================================
*/
-- 5.1 数据清洗与摄入
INSERT INTO ods.ods_kafka_app_click
SELECT
    -- 净土清洗：非空处理
    COALESCE(uid, 'UNKNOWN') AS user_id,
    COALESCE(item_id, '-1') AS sku_id,
    UPPER(action) AS behavior,
    ext_info AS raw_json,
    -- 元数据注入
    event_ts AS _event_time,
    CURRENT_TIMESTAMP AS _ingest_time,
    DATE_FORMAT(event_ts, 'yyyyMMdd') AS _partition_dt,
    DATE_FORMAT(event_ts, 'HH') AS _partition_hh,
    kafka_offset AS _kafka_offset,
    kafka_partition AS _kafka_partition
FROM kafka_app_click_source
WHERE uid IS NOT NULL;  -- 核心清洗：过滤无用户 ID 的非法流量
```

### 2.2 CDC 接入作业模板

```sql
/*
============================================================
作业说明：ODS 层订单明细 CDC 接入
作业类型：ODS 层 CDC 接入
数据流向：MySQL CDC -> Paimon ODS 表
============================================================
*/

-- 1. CDC 源表定义
CREATE TEMPORARY TABLE cdc_sale_order_item (
    order_id STRING,
    line_no INT,
    sku_id STRING,
    qty DECIMAL(12, 2),
    sale_price DECIMAL(18, 2),
    sale_amount DECIMAL(18, 2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id, line_no) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.0.102',
    'port' = '3306',
    'username' = 'root',
    'password' = 'xxx',
    'database-name' = 'erp_db',
    'table-name' = 'sale_order_item',
    'scan.startup.mode' = 'initial'
);

-- 2. ODS 目标表定义
CREATE TABLE IF NOT EXISTS ods_order_item_cdc (
    order_id STRING,
    line_no INT,
    sku_id STRING,
    qty DECIMAL(12, 2),
    sale_price DECIMAL(18, 2),
    sale_amount DECIMAL(18, 2),
    order_time TIMESTAMP(3),
    _event_time TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _partition_dt STRING,
    PRIMARY KEY (order_id, line_no, _partition_dt) NOT ENFORCED
) PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = '_ingest_time'
);

-- 3. ETL 逻辑
INSERT INTO ods_order_item_cdc
SELECT
    i.order_id,
    i.line_no,
    i.sku_id,
    i.qty,
    i.sale_price,
    i.sale_amount,
    o.order_time,
    COALESCE(o.order_time, i.updated_at) AS _event_time,
    CURRENT_TIMESTAMP AS _ingest_time,
    DATE_FORMAT(COALESCE(o.order_time, i.updated_at), 'yyyyMMdd') AS _partition_dt
FROM cdc_sale_order_item i
LEFT JOIN cdc_sale_order o
ON i.order_id = o.order_id;
```

---

## 三、JOIN 规范

### 3.1 维度关联语法

```sql
-- ? 正确：使用 FOR SYSTEM_TIME AS OF 进行时态关联
SELECT
    o.order_id,
    o.sku_id,
    d.sku_name,
    d.brand_name,
    d.season
FROM ods.ods_order_item_cdc o
LEFT JOIN dim.dim_sku_wide FOR SYSTEM_TIME AS OF o._event_time AS d
ON o.sku_id = d.sku_id;
```

### 3.2 JOIN 类型选择

| JOIN 类型 | 使用场景 | 说明 |
|---------|---------|------|
| LEFT JOIN | 维度关联 | 事实表 LEFT JOIN 维度表 |
| INNER JOIN | 事实表关联 | 两表都有数据才保留 |
| Temporal JOIN | 时态关联 | 使用 FOR SYSTEM_TIME AS OF |

### 3.3 JOIN 注意事项

?? **JOIN 键必须有时间语义**

```sql
-- ? 正确：维度关联带时间语义
SELECT ...
FROM ods_order o
LEFT JOIN dim_sku FOR SYSTEM_TIME AS OF o._event_time AS d
ON o.sku_id = d.sku_id;

-- ? 错误：不带时间语义的 JOIN
SELECT ...
FROM ods_order o
LEFT JOIN dim_sku d
ON o.sku_id = d.sku_id;
```

---

## 四、作业配置规范

### 4.1 必须配置项

```sql
-- 执行模式
SET 'execution.runtime-mode' = 'streaming';

-- 检查点间隔（必须）
SET 'execution.checkpointing.interval' = '10s';
```

### 4.2 推荐配置项

```sql
-- 检查点超时
SET 'execution.checkpointing.timeout' = '10min';

-- 检查点模式
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 重启策略
SET 'execution.restaurant.strategy' = 'failure-rate';
SET 'execution.restaurant.failure-rate.max-failures-per-interval' = '3';
SET 'execution.restaurant.failure-rate.interval' = '5min';
SET 'execution.restaurant.failure-rate.delay' = '30s';

-- 状态后端（生产环境推荐）
-- SET 'state.backend' = 'rocksdb';
-- SET 'state.backend.incremental' = 'true';
-- SET 'state.ttl' = '7d';
```

---

## 五、错误处理规范

### 5.1 JSON 解析容错

```sql
-- 必须配置容错
WITH (
    ...
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);
```

### 5.2 空值处理

```sql
-- 主键字段非空处理
COALESCE(uid, 'UNKNOWN') AS user_id
COALESCE(item_id, '-1') AS sku_id

-- 业务字段可空
COALESCE(discount_amount, 0) AS discount_amount
```

### 5.3 数据过滤

```sql
-- 过滤非法数据
WHERE uid IS NOT NULL
  AND sku_id IS NOT NULL
  AND sku_id != '-1'
```

---

## 六、代码注释规范

### 6.1 注释要求

| 位置 | 注释内容 |
|-----|---------|
| 文件头部 | 作业说明、作业类型、数据流向、创建信息 |
| 步骤之间 | 步骤说明 |
| 复杂逻辑 | 核心逻辑说明 |
| 关键字段 | 字段用途说明 |

### 6.2 注释示例

```sql
-- 5.1 核心清洗逻辑
-- 说明：过滤无用户 ID 的非法流量，防止下游 JOIN 倾斜
WHERE uid IS NOT NULL

-- 5.2 分区路由
-- 说明：基于事件时间生成分区，解决迟到数据分区错误问题
DATE_FORMAT(event_ts, 'yyyyMMdd') AS _partition_dt
```

---

## 七、Flink SQL 自检清单

### 7.1 作业结构检查

- [ ] 文件头部注释完整
- [ ] 步骤注释清晰
- [ ] 配置项完整（checkpointing）

### 7.2 源表检查

- [ ] Kafka Source 已声明 WATERMARK
- [ ] CDC Source 已声明 PRIMARY KEY
- [ ] JSON 容错配置正确
- [ ] 消费者组命名规范

### 7.3 目标表检查

- [ ] 表名符合分层命名规范
- [ ] 主键包含所有分区字段
- [ ] Merge Engine 选择正确
- [ ] Bucket 配置合理

### 7.4 ETL 逻辑检查

- [ ] 元数据字段已注入
- [ ] 分区字段基于事件时间生成
- [ ] 空值处理完善
- [ ] 数据过滤合理

### 7.5 维度关联检查

- [ ] 使用 FOR SYSTEM_TIME AS OF 语法
- [ ] JOIN 键有时间语义

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 05-watermark-partition.md | 水位与分区规范 |
| 07-datastream.md | DataStream API 开发规范 |
| 08-paimon.md | Paimon 存储规范 |

---

> 本文档定义了 Flink SQL 作业开发规范，是 Flink SQL 开发的核心参考。
