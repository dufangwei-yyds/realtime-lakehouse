# 数据质量与验证规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：数据质量检查、验证方法、异常处理  
> 适用范围：ODS/DIM/DWD/DWS/ADS 全链路数据质量检查

---

## 一、数据质量维度

### 1.1 六大质量维度

| 维度 | 说明 | 检查方法 |
|-----|------|---------|
| 完整性 | 数据是否完整，无缺失 | NULL 检查、计数验证 |
| 准确性 | 数据是否准确，符合业务 | 抽样比对、阈值校验 |
| 一致性 | 数据是否一致，无矛盾 | 跨表比对、汇总验证 |
| 时效性 | 数据是否及时，无延迟 | 时间戳检查、延迟监控 |
| 唯一性 | 数据是否唯一，无重复 | 主键去重检查 |
| 有效性 | 数据是否有效，符合规范 | 枚举值检查、格式校验 |

---

## 二、完整性检查

### 2.1 NULL 值检查

```sql
-- 检查主键字段 NULL
SELECT COUNT(*) AS null_order_id_cnt
FROM ods_order_item_cdc
WHERE order_id IS NULL;

-- 检查业务关键字段 NULL
SELECT sku_id, COUNT(*) AS null_cnt
FROM ods_order_item_cdc
WHERE sku_id IS NULL OR sku_id = '-1'
GROUP BY sku_id;
```

### 2.2 计数验证

```sql
-- ODS 层计数
SELECT 
    _partition_dt,
    COUNT(*) AS row_cnt,
    COUNT(DISTINCT order_id) AS order_cnt
FROM ods_order_item_cdc
WHERE _partition_dt = '20251224'
GROUP BY _partition_dt;

-- 与 CDC 源表计数比对
-- 预期：ODS 计数 ≈ CDC 源表计数（允许少量延迟差异）
```

---

## 三、准确性检查

### 3.1 数值范围检查

```sql
-- 检查金额合理性
SELECT COUNT(*) AS invalid_amount_cnt
FROM ods_order_item_cdc
WHERE sale_amount <= 0 OR sale_amount > 999999999;

-- 检查数量合理性
SELECT COUNT(*) AS invalid_qty_cnt
FROM ods_order_item_cdc
WHERE qty <= 0 OR qty > 999999;
```

### 3.2 枚举值检查

```sql
-- 检查订单状态枚举
SELECT order_status, COUNT(*) AS cnt
FROM ods_order_item_cdc
GROUP BY order_status;

-- 预期状态值：CREATED, PAID, SHIPPED, SIGNED, CLOSED, REFUNDING, REFUNDED
```

### 3.3 抽样比对

```sql
-- 随机抽样 10 条比对
SELECT *
FROM ods_order_item_cdc
WHERE _partition_dt = '20251224'
ORDER BY RAND()
LIMIT 10;

-- 比对字段：
-- 1. order_id 与业务库一致
-- 2. sku_id 与业务库一致
-- 3. sale_amount 与业务库一致
```

---

## 四、一致性检查

### 4.1 跨表数据比对

```sql
-- DWD 层与 ODS 层计数比对
SELECT 
    'ODS' AS layer,
    COUNT(*) AS row_cnt,
    COUNT(DISTINCT order_id) AS order_cnt
FROM ods_order_item_cdc
WHERE _partition_dt = '20251224'

UNION ALL

SELECT 
    'DWD' AS layer,
    COUNT(*) AS row_cnt,
    COUNT(DISTINCT order_id) AS order_cnt
FROM dwd_order_trade_detail_rt
WHERE _partition_dt = '20251224';
```

### 4.2 汇总数据验证

```sql
-- 验证 DWS 汇总与 DWD 明细一致
SELECT 
    SUM(d.qty) AS dwd_qty,
    w.sku_hh_qty AS dws_qty,
    ABS(SUM(d.qty) - w.sku_hh_qty) AS diff
FROM dwd_order_trade_detail_rt d
JOIN (
    SELECT sku_id, SUM(qty) AS sku_hh_qty
    FROM dws_sales_sku_hh_rt
    WHERE _partition_dt = '20251224'
    GROUP BY sku_id
) w ON d.sku_id = w.sku_id
WHERE d._partition_dt = '20251224'
GROUP BY w.sku_hh_qty
HAVING diff > 0;
```

---

## 五、时效性检查

### 5.1 延迟监控

```sql
-- 检查数据延迟
SELECT 
    MAX(_event_time) AS latest_event_time,
    MAX(_ingest_time) AS latest_ingest_time,
    TIMESTAMPDIFF(MINUTE, MAX(_event_time), NOW()) AS event_delay_min,
    TIMESTAMPDIFF(MINUTE, MAX(_ingest_time), NOW()) AS ingest_delay_min
FROM ods_order_item_cdc
WHERE _partition_dt = DATE_FORMAT(NOW(), 'yyyyMMdd');

-- ?? 告警阈值：
-- - 事件延迟 > 30 分钟
-- - 入湖延迟 > 10 分钟
```

### 5.2 分区连续性检查

```sql
-- 检查分区是否连续
SELECT 
    _partition_dt,
    COUNT(*) AS row_cnt
FROM ods_kafka_app_click
WHERE _partition_dt >= DATE_FORMAT(NOW() - INTERVAL '7' DAY, 'yyyyMMdd')
GROUP BY _partition_dt
ORDER BY _partition_dt;

-- ?? 检查项：
-- - 是否有缺失分区
-- - 是否有空分区
-- - 数据量是否异常
```

---

## 六、唯一性检查

### 6.1 主键重复检查

```sql
-- 检查主键重复
SELECT 
    order_id,
    line_no,
    _partition_dt,
    COUNT(*) AS repeat_cnt
FROM ods_order_item_cdc
WHERE _partition_dt = '20251224'
GROUP BY order_id, line_no, _partition_dt
HAVING COUNT(*) > 1;
```

### 6.2 Kafka 重复检查

```sql
-- 检查 Kafka 消息重复
SELECT 
    _kafka_offset,
    _kafka_partition,
    COUNT(*) AS repeat_cnt
FROM ods_kafka_app_click_dedup
WHERE _partition_dt = '20251224'
GROUP BY _kafka_offset, _kafka_partition
HAVING COUNT(*) > 1;
```

---

## 七、验证方法论

### 7.1 分层验证策略

| 层级 | 验证重点 | 验证方法 |
|-----|---------|---------|
| ODS | 数据完整、原始保留 | NULL 检查、计数比对 |
| DIM | 维度口径一致 | 枚举值检查、属性标准化 |
| DWD | 事实打宽正确 | JOIN 结果验证、维度属性正确 |
| DWS | 指标计算正确 | 汇总验证、精度校验 |
| ADS | 结果符合消费需求 | 业务口径验证 |

### 7.2 验证时机

| 阶段 | 验证时机 |
|-----|---------|
| 开发阶段 | 作业开发完成时 |
| 测试阶段 | 联调验证前 |
| 发布阶段 | 发布后 30 分钟内 |
| 日常运维 | 巡检时 |

---

## 八、质量告警规则

### 8.1 告警阈值

| 告警项 | 阈值 | 级别 |
|-------|------|-----|
| 主键重复率 | > 1% | 严重 |
| NULL 率 | > 5% | 警告 |
| 数据延迟 | > 30 分钟 | 警告 |
| 数据量异常 | 偏离均值 > 50% | 警告 |
| 枚举值异常 | 出现未定义值 | 警告 |

### 8.2 告警处理流程

```
告警触发
    ↓
定位问题（ODS/DIM/DWD/DWS/ADS）
    ↓
判断是否需要回滚
    ↓
执行回滚或修复
    ↓
验证修复结果
    ↓
记录问题根因
```

---

## 九、数据质量自检清单

### 9.1 开发自检

- [ ] NULL 值检查 SQL 已准备
- [ ] 枚举值检查 SQL 已准备
- [ ] 主键重复检查 SQL 已准备
- [ ] 计数比对 SQL 已准备

### 9.2 发布自检

- [ ] 数据延迟检查通过
- [ ] 数据量检查通过
- [ ] 主键重复率检查通过
- [ ] 枚举值检查通过

---

## 十、后续文档索引

| 文档 | 定位 |
|-----|------|
| 06-flink-sql.md | Flink SQL 开发规范 |
| 08-paimon.md | Paimon 存储规范 |
| 09-doris.md | Doris 开发规范 |
| 11-deploy.md | 发布与回滚规范 |

---

> 本文档定义了数据质量检查规范和验证方法，是数据质量保障的核心参考。
