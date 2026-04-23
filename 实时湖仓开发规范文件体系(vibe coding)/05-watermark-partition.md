# 水位与分区规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Watermark 水位策略、分区设计、分区字段规范  
> 适用范围：Flink SQL 窗口计算、Paimon 表分区设计、数据治理

---

## 一、Watermark 水位规范

### 1.1 水位线概念

**Watermark（水位线）** 是 Flink 用于处理乱序和迟到数据的机制：

- **乱序数据**：事件到达顺序与事件发生顺序不一致
- **迟到数据**：事件在窗口结束后才到达
- **Watermark**：表示"时间 X 之前的数据都已到达"，触发窗口计算

### 1.2 水位策略设计原则

| 原则 | 说明 |
|-----|------|
| 事件时间优先 | 必须使用业务事件时间（_event_time）作为时间字段 |
| 延迟容忍 | 水位延迟 = 允许的最大乱序/迟到时间 |
| 平衡延迟与准确性 | 延迟太长影响实时性，太短可能丢失有效数据 |

### 1.3 推荐水位策略

**行为事件流**
```sql
WATERMARK FOR _event_time AS _event_time - INTERVAL '5' MINUTE
```
- 适用：APP 点击流、页面浏览
- 理由：埋点数据可能有 5 分钟以内的乱序

**业务库 CDC 数据**
```sql
WATERMARK FOR _event_time AS _event_time - INTERVAL '1' MINUTE
```
- 适用：订单、库存变更
- 理由：MySQL binlog 延迟通常在秒级，1 分钟足够

**第三方 API 数据**
```sql
WATERMARK FOR _event_time AS _event_time - INTERVAL '10' MINUTE
```
- 适用：物流轨迹、社媒数据
- 理由：第三方 API 可能有较大延迟

### 1.4 水位声明规范

? **以下写法严格禁止**

```sql
-- ? 禁止：使用处理时间
WATERMARK FOR proctime AS proctime - INTERVAL '5' MINUTE

-- ? 禁止：不声明 WATERMARK
-- 没有 WATERMARK，无法处理乱序和迟到数据

-- ? 禁止：负延迟（提前触发）
WATERMARK FOR _event_time AS _event_time + INTERVAL '5' MINUTE
```

? **正确写法**

```sql
CREATE TEMPORARY TABLE kafka_source (
    ...
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
) WITH (
    ...
);
```

---

## 二、分区设计规范

### 2.1 分区类型

| 分区类型 | 字段 | 格式 | 适用场景 |
|---------|------|------|---------|
| 日期分区 | _partition_dt | yyyyMMdd | 所有表 |
| 小时分区 | _partition_hh | HH | 行为流、高频事件 |
| 天小时分区 | _partition_dt + _partition_hh | 二级分区 | 大促、实时监控 |

### 2.2 日期分区规范

**字段定义**
```sql
_partition_dt STRING  -- 格式：yyyyMMdd
```

**生成规则**
```sql
DATE_FORMAT(_event_time, 'yyyyMMdd') AS _partition_dt
```

**分区裁剪**
```sql
-- ? 正确：分区字段参与计算
WHERE _partition_dt = '20251224'

-- ? 错误：分区字段参与函数计算，无法裁剪
WHERE DATE_FORMAT(CAST(_partition_dt AS DATE), 'yyyy-MM-dd') = '2025-12-24'
```

### 2.3 小时分区规范

**字段定义**
```sql
_partition_hh STRING  -- 格式：HH（00-23）
```

**生成规则**
```sql
DATE_FORMAT(_event_time, 'HH') AS _partition_hh
```

**适用场景**

| 场景 | 是否需要小时分区 | 理由 |
|-----|----------------|------|
| APP 点击流 | ? 必须 | 大促期间数据量大，需二级分区 |
| 页面浏览流 | ? 必须 | 数据量大，需要小时级聚合 |
| 订单事实 | ?? 推荐 | 订单量适中，小时分区有助于实时分析 |
| 库存流水 | ?? 推荐 | 需要小时级库存变动监控 |
| 商品维度 | ? 不需要 | 变化频率低，天分区足够 |
| 门店维度 | ? 不需要 | 变化频率低，天分区足够 |

### 2.4 二级分区声明

**Paimon 表分区声明**
```sql
CREATE TABLE ods_kafka_app_click (
    user_id STRING,
    sku_id STRING,
    behavior STRING,
    _event_time TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _partition_dt STRING,
    _partition_hh STRING,
    WATERMARK FOR _event_time AS _event_time - INTERVAL '5' MINUTE
) PARTITIONED BY (_partition_dt, _partition_hh)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'append-only'
);
```

---

## 三、分区与时间语义

### 3.1 事件时间分区原则

? **分区字段必须与事件时间语义一致**

```sql
-- ? 正确：基于业务事件时间生成分区
INSERT INTO ods_kafka_app_click
SELECT
    user_id,
    _event_time,
    DATE_FORMAT(_event_time, 'yyyyMMdd') AS _partition_dt,
    DATE_FORMAT(_event_time, 'HH') AS _partition_hh
FROM kafka_source;
```

? **错误：基于入湖时间生成分区（语义错误）**

```sql
-- ? 错误：使用入湖时间生成分区，导致迟到数据进入错误分区
INSERT INTO ods_kafka_app_click
SELECT
    user_id,
    _event_time,
    DATE_FORMAT(_ingest_time, 'yyyyMMdd') AS _partition_dt,  -- 错误！
    DATE_FORMAT(_ingest_time, 'HH') AS _partition_hh          -- 错误！
FROM kafka_source;
```

### 3.2 迟到数据分区处理

**问题场景**：事件时间 14:55 的数据，15:10 才到达

**错误处理**：数据进入 15 分区
**正确处理**：数据仍然进入 14 分区（基于 _event_time）

```sql
-- 正确：分区基于 _event_time
INSERT INTO ods_kafka_app_click (...)
SELECT
    ...
    DATE_FORMAT(event_time, 'yyyyMMdd') AS _partition_dt,  -- ? 基于事件时间
    DATE_FORMAT(event_time, 'HH') AS _partition_hh        -- ? 基于事件时间
FROM kafka_source;
```

---

## 四、分区管理规范

### 4.1 分区字段不可修改

?? **重要规则**

分区字段一旦写入，不可修改：
- ? 禁止 UPDATE 分区字段
- ? 禁止 ALTER TABLE 修改分区字段类型

### 4.2 分区过期清理

**Paimon 分区清理**
```sql
-- 删除指定分区
CALL sys.delete_partitions('paimon_catalog.ods.ods_kafka_app_click', 'dt=20251101');

-- 批量删除过期分区
-- 建议保留 90 天历史数据
```

### 4.3 分区监控

| 监控项 | 指标 | 告警阈值 |
|-------|------|---------|
| 分区数据量 | 每个分区行数 | 单分区 > 1 亿行告警 |
| 分区延迟 | 事件时间与当前时间差 | > 2 小时告警 |
| 分区空洞 | 缺失分区 | 连续 3 个分区无数据告警 |

---

## 五、窗口计算规范

### 5.1 窗口类型

| 窗口类型 | 说明 | 适用场景 |
|---------|------|---------|
| 滚动窗口 (Tumble) | 固定大小，不重叠 | 每小时汇总、每天汇总 |
| 滑动窗口 (Slide) | 固定大小，可重叠 | 需要连续趋势数据 |
| 会话窗口 (Session) | 活动间隙分隔 | 用户行为分析 |

### 5.2 滚动窗口示例

**小时级汇总**
```sql
SELECT
    DATE_FORMAT(TUMBLE_START(_event_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss') AS window_start,
    sku_id,
    COUNT(*) AS click_cnt,
    COUNT(DISTINCT user_id) AS uv
FROM ods_kafka_app_click
WHERE behavior = 'CLICK'
GROUP BY
    TUMBLE(_event_time, INTERVAL '1' HOUR),
    sku_id;
```

**天级汇总**
```sql
SELECT
    DATE_FORMAT(TUMBLE_START(_event_time, INTERVAL '1' DAY), 'yyyy-MM-dd') AS window_start,
    sku_id,
    SUM(sale_amount) AS daily_sales
FROM dwd_order_trade_detail_rt
WHERE order_status = 'SIGNED'
GROUP BY
    TUMBLE(_event_time, INTERVAL '1' DAY),
    sku_id;
```

### 5.3 滑动窗口示例

**15 分钟滚动、5 分钟步长**
```sql
SELECT
    HOP_START(_event_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS window_start,
    sku_id,
    COUNT(*) AS click_cnt
FROM ods_kafka_app_click
GROUP BY
    HOP(_event_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE),
    sku_id;
```

### 5.4 窗口边界处理

```sql
-- 窗口触发时机
-- 水位线达到窗口结束时间时，窗口触发计算
-- 之后到达的迟到数据（窗口结束前）会被二次处理

-- 设置允许迟到时间
SELECT
    sku_id,
    TUMBLE_START(_event_time, INTERVAL '1' HOUR) AS window_start,
    COUNT(*) AS click_cnt
FROM ods_kafka_app_click
GROUP BY
    TUMBLE(_event_time, INTERVAL '1' HOUR),
    sku_id
-- 允许 5 分钟迟到数据
WITHIN WATERMARK MARK
```

---

## 六、水位与分区检查清单

### 6.1 水位检查

- [ ] Kafka Source 已声明 WATERMARK
- [ ] WATERMARK 基于事件时间（_event_time）
- [ ] WATERMARK 延迟设置合理（5分钟/1分钟/10分钟）
- [ ] 未使用处理时间作为 WATERMARK

### 6.2 分区检查

- [ ] 分区字段类型为 STRING
- [ ] 分区字段格式正确（yyyyMMdd / HH）
- [ ] 分区基于事件时间生成（不是入湖时间）
- [ ] 行为流已声明二级分区（dt + hh）
- [ ] 分区字段在 WHERE 条件中可直接使用（无函数包裹）

### 6.3 窗口检查

- [ ] 窗口函数使用 _event_time
- [ ] 窗口大小符合业务需求
- [ ] 窗口输出包含 window_start / window_end 标识

---

## 七、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 04-field.md | 字段与数据类型规范 |
| 06-flink-sql.md | Flink SQL 开发规范 |
| 08-paimon.md | Paimon 存储规范 |

---

> 本文档定义了 Watermark 水位策略和分区设计规范，是实时计算和数据存储的核心参考。
