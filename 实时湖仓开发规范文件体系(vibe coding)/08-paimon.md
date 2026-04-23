# Paimon 存储规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Paimon 表设计规范、存储配置、Merge Engine 选择  
> 适用范围：Paimon 表结构设计、分区设计、存储优化

---

## 一、Paimon 表类型

### 1.1 写入模式

| 写入模式 | 说明 | 适用场景 |
|---------|------|---------|
| append-only | 仅追加模式 | 纯事件流、高吞吐写入 |
| change-log-upsert | 变更日志模式 | CDC 数据、需要完整变更记录 |

### 1.2 Merge Engine

| Merge Engine | 说明 | 适用场景 |
|-------------|------|---------|
| deduplicate | 去重合并 | 订单、库存等需要唯一性的场景 |
| partial-update | 部分更新 | 拉链表、多源更新场景 |
| aggregation | 聚合合并 | 需要对同键数据进行聚合的场景 |
| first-row | 首行保留 | 仅保留第一条记录 |

---

## 二、表设计规范

### 2.1 Append 表设计

**适用场景**：纯事件流、行为埋点、日志数据

```sql
CREATE TABLE ods_kafka_app_click (
    user_id STRING,
    sku_id STRING,
    behavior STRING,
    raw_json STRING,
    _event_time TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _partition_dt STRING,
    _partition_hh STRING,
    WATERMARK FOR _event_time AS _event_time - INTERVAL '5' SECOND
) PARTITIONED BY (_partition_dt, _partition_hh)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'append-only',     -- 追加模式
    'bucket' = '-1',                  -- 动态分桶
    'file.format' = 'orc'            -- 文件格式
);
```

### 2.2 主键表设计

**适用场景**：需要唯一性、去重的场景

```sql
CREATE TABLE ods_order_item_cdc (
    order_id STRING,
    line_no INT,
    sku_id STRING,
    qty DECIMAL(12, 2),
    sale_amount DECIMAL(18, 2),
    _event_time TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _partition_dt STRING,
    PRIMARY KEY (order_id, line_no, _partition_dt) NOT ENFORCED
) PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'change-log-upsert',
    'merge-engine' = 'deduplicate',   -- 去重合并
    'bucket' = '4',                    -- 固定分桶数
    'sequence.field' = '_ingest_time', -- 使用入湖时间排序
    'file.format' = 'orc'
);
```

?? **重要规则**

主键必须包含所有分区字段：
```sql
-- ? 正确
PRIMARY KEY (order_id, line_no, _partition_dt)

-- ? 错误
PRIMARY KEY (order_id, line_no)
```

### 2.3 SCD2 拉链表设计

**适用场景**：维度历史变化保留

```sql
CREATE TABLE dim_product_history (
    sku_id STRING,
    sku_name STRING,
    brand_name STRING,
    start_date STRING,                 -- 有效期开始
    end_date STRING,                   -- 有效期结束（99991231 表示当前）
    _event_time TIMESTAMP(3),
    PRIMARY KEY (sku_id, start_date) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'merge-engine' = 'aggregate',     -- 聚合合并
    'fields.start_date.aggregate-function' = 'max',
    'fields.end_date.aggregate-function' = 'max',
    'bucket' = '4',
    'file.format' = 'orc'
);
```

---

## 三、分区设计规范

### 3.1 分区字段规范

| 规则 | 说明 |
|-----|------|
| 分区字段类型 | 必须是 STRING |
| 日期分区格式 | yyyyMMdd |
| 小时分区格式 | HH |
| 分区数量 | 建议不超过 2 级 |

### 3.2 分区声明

```sql
-- 单分区（天）
CREATE TABLE ods_order_cdc (...)
PARTITIONED BY (_partition_dt)

-- 二级分区（天+小时）
CREATE TABLE ods_app_click (...)
PARTITIONED BY (_partition_dt, _partition_hh)
```

### 3.3 分区字段规则

? **以下写法严格禁止**

- 分区字段不能参与计算
- 分区字段不能有默认值
- 分区字段不能更新

---

## 四、存储配置规范

### 4.1 Bucket 配置

| 表类型 | Bucket 配置 | 说明 |
|-------|------------|------|
| Append 表 | bucket = '-1' | 动态分桶，自动适配数据量 |
| 主键表 | bucket = '4' | 固定分桶数，需根据数据量调整 |

### 4.2 文件格式配置

```sql
-- 推荐使用 ORC
'file.format' = 'orc'

-- 可选 Parquet
'file.format' = 'parquet'
```

### 4.3 压缩配置

```sql
-- ORC 压缩
'file.compression' = 'ZSTD'  -- 推荐，压缩率高

-- Parquet 压缩
'file.parquet.compression' = 'ZSTD'
```

---

## 五、数据查询规范

### 5.1 分区裁剪查询

```sql
-- ? 正确：分区字段直接使用
SELECT * FROM ods_order_cdc
WHERE _partition_dt = '20251224';

-- ? 错误：分区字段参与计算
SELECT * FROM ods_order_cdc
WHERE DATE_FORMAT(CAST(_partition_dt AS DATE), 'yyyy-MM-dd') = '2025-12-24';
```

### 5.2 时态查询

```sql
-- 查询某个时间点的维度快照
SELECT * FROM dim_sku_wide
WHERE sku_id = 'SKU001'
AND _event_time <= TIMESTAMP '2025-12-24 00:00:00'
ORDER BY _event_time DESC
LIMIT 1;
```

### 5.3 分区过期查询

```sql
-- 查询最近 7 天数据
SELECT * FROM ods_order_cdc
WHERE _partition_dt >= DATE_FORMAT(CURRENT_DATE - INTERVAL '7' DAY, 'yyyyMMdd');
```

---

## 六、分区管理规范

### 6.1 分区删除

```sql
-- 删除单个分区
CALL sys.delete_partitions('paimon_catalog.ods.ods_order_cdc', 'dt=20251101');

-- 删除多个分区
CALL sys.delete_partitions('paimon_catalog.ods.ods_order_cdc', 'dt<20251101');
```

### 6.2 分区过期策略

| 表类型 | 保留周期 | 说明 |
|-------|---------|------|
| ODS 原始表 | 90 天 | 保留原始数据，支持回溯 |
| ODS 解析表 | 90 天 | 解析后的明细数据 |
| DIM 维度表 | 永久 | 维度数据需要长期保留 |
| DWD 明细表 | 365 天 | 明细数据长期保留 |
| DWS 汇总表 | 730 天 | 汇总数据长期保留 |

---

## 七、Paimon 表自检清单

### 7.1 表结构检查

- [ ] 表名符合分层命名规范
- [ ] 主键声明正确（包含所有分区字段）
- [ ] 分区字段类型正确（STRING）
- [ ] 字段类型符合规范

### 7.2 存储配置检查

- [ ] Merge Engine 选择正确
- [ ] Bucket 配置合理
- [ ] 文件格式正确（ORC）

### 7.3 元数据检查

- [ ] _event_time 字段存在
- [ ] _ingest_time 字段存在
- [ ] _partition_dt 字段存在
- [ ] WATERMARK 已声明

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 05-watermark-partition.md | 水位与分区规范 |
| 06-flink-sql.md | Flink SQL 开发规范 |
| 10-quality.md | 数据质量与验证规范 |

---

> 本文档定义了 Paimon 表设计规范和存储配置，是 Paimon 表开发的核心参考。
