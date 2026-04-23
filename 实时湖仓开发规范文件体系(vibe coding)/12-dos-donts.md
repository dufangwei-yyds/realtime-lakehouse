# 明确禁止项

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：实时湖仓开发中必须避免的错误行为  
> 适用范围：ODS/DIM/DWD/DWS/ADS 全链路开发、作业编写、表设计

---

## 一、分层边界禁止项

### 1.1 ODS 层禁止

? **禁止在 ODS 层做重度业务聚合**

```sql
-- ? 错误示例
INSERT INTO ods_order_summary
SELECT
    DATE_FORMAT(order_time, 'yyyyMMdd') AS dt,
    COUNT(*) AS order_cnt,
    SUM(sale_amount) AS total_amount  -- 违反：ODS 不做重度聚合
FROM cdc_sale_order
GROUP BY DATE_FORMAT(order_time, 'yyyyMMdd');
```

? **正确做法**：在 DWS 层做聚合

---

? **禁止跳过 ODS 直连 DIM 层**

```sql
-- ? 错误示例
INSERT INTO dim_sku_wide
SELECT
    c.sku_id,
    c.sku_name
FROM cdc_m_product c  -- 违反：跳过 ODS 直连 DIM
LEFT JOIN dim_category d ON c.category_id = d.category_id;
```

? **正确做法**：ODS -> DIM -> DIM

---

? **禁止不注入元数据字段**

```sql
-- ? 错误示例
INSERT INTO ods_order_item_cdc
SELECT
    order_id,
    line_no,
    sku_id,
    qty,
    sale_amount
    -- 违反：缺少 _event_time、_ingest_time、_partition_dt
FROM cdc_sale_order_item;
```

? **正确做法**：必须注入所有元数据字段

---

### 1.2 DIM 层禁止

? **禁止维度主键跨作业不一致**

```sql
-- ? 错误示例
-- 作业 A
INSERT INTO dim_sku_wide
SELECT
    sku_id AS sku_code,  -- 违反：字段名不一致
    ...
FROM ods_m_product_latest;

-- 作业 B
INSERT INTO dwd_order_trade_detail_rt
SELECT
    sku_id,  -- 与 dim_sku_wide.sku_code 无法关联
    ...
```

? **正确做法**：所有作业使用统一的维度主键

---

? **禁止在 DIM 层做事实聚合**

```sql
-- ? 错误示例
INSERT INTO dim_sku_wide
SELECT
    sku_id,
    SUM(sale_amount) AS total_sales  -- 违反：维度层不做聚合
FROM ods_order_item
GROUP BY sku_id;
```

? **正确做法**：在 DWS 层做聚合

---

### 1.3 DWD 层禁止

? **禁止将多个业务过程混在一张表**

```sql
-- ? 错误示例
CREATE TABLE dwd_trade_all_in_one (  -- 违反：多业务过程混在一起
    ...
    -- 订单支付
    -- 库存变动
    -- 物流发货
    -- 全部混在一起
);
```

? **正确做法**：一个业务过程一张表

---

? **禁止使用处理时间代替业务事件时间**

```sql
-- ? 错误示例
INSERT INTO dwd_order_trade_detail_rt
SELECT
    order_id,
    ...
    CURRENT_TIMESTAMP AS _event_time  -- 违反：使用处理时间
FROM ods_order_item;
```

? **正确做法**：使用业务事件时间

---

### 1.4 DWS 层禁止

? **禁止粒度不清**

```sql
-- ? 错误示例
CREATE TABLE dws_sales_mix_rt (  -- 违反：混合不同粒度
    sku_id STRING,       -- SKU 粒度
    store_id STRING,     -- 门店粒度
    sale_amt DECIMAL(18,2)  -- 无法明确粒度
    -- 应该分开：dws_sales_sku_rt 和 dws_sales_store_rt
);
```

? **正确做法**：每个表一个明确的粒度

---

? **禁止派生指标在 DWS 层定义**

```sql
-- ? 错误示例
CREATE TABLE dws_sales_sku_rt (
    sku_id STRING,
    sale_amt DECIMAL(18,2),         -- 原子指标 OK
    conversion_rate DECIMAL(10,4)     -- 违反：派生指标应在 ADS 层
);
```

? **正确做法**：派生指标在 ADS 层组合

---

## 二、技术实现禁止项

### 2.1 字段类型禁止

? **禁止使用 TIMESTAMP(0) 或 DATETIME**

```sql
-- ? 错误示例
order_time TIMESTAMP(0)  -- 违反：精度不足
create_time DATETIME      -- 违反：与 Flink 时间语义不统一
```

? **正确做法**：使用 TIMESTAMP(3)

---

? **禁止金额使用 FLOAT/DOUBLE**

```sql
-- ? 错误示例
sale_amount DOUBLE  -- 违反：精度问题
```

? **正确做法**：使用 DECIMAL(18,2)

---

### 2.2 WATERMARK 禁止

? **禁止使用处理时间作为 WATERMARK**

```sql
-- ? 错误示例
WATERMARK FOR proctime AS proctime - INTERVAL '5' MINUTE
```

? **正确做法**：使用事件时间

---

? **禁止不声明 WATERMARK**

```sql
-- ? 错误示例
CREATE TEMPORARY TABLE kafka_source (
    event_time TIMESTAMP(3)
    -- 违反：缺少 WATERMARK 声明
);
```

? **正确做法**：必须声明 WATERMARK

---

### 2.3 分区字段禁止

? **禁止分区字段参与计算**

```sql
-- ? 错误示例
SELECT * FROM ods_order
WHERE DATE_FORMAT(CAST(_partition_dt AS DATE), 'yyyy-MM-dd') = '2025-12-24'
-- 违反：分区字段参与计算，无法裁剪
```

? **正确做法**：分区字段直接使用

---

? **禁止使用入湖时间生成分区**

```sql
-- ? 错误示例
INSERT INTO ods_kafka_app_click
SELECT
    ...
    DATE_FORMAT(_ingest_time, 'yyyyMMdd') AS _partition_dt  -- 违反：基于入湖时间
FROM kafka_source;
```

? **正确做法**：基于事件时间生成分区

---

## 三、命名禁止项

### 3.1 表名禁止

? **禁止使用中文表名**

```sql
-- ? 错误示例
CREATE TABLE ods_订单明细 (...)  -- 违反：中文表名
```

? **正确做法**：使用英文小写下划线分隔

---

? **禁止缺少分层前缀**

```sql
-- ? 错误示例
CREATE TABLE sale_order (...)  -- 违反：缺少 ods_ 前缀
```

? **正确做法**：表名必须包含分层前缀

---

### 3.2 字段名禁止

? **禁止混用大小写**

```sql
-- ? 错误示例
SELECT order_id, OrderID, ORDER_ID FROM ...  -- 违反：大小写不一致
```

? **正确做法**：统一小写

---

? **禁止使用保留字作为字段名**

```sql
-- ? 错误示例
SELECT date, time, table, key FROM ...  -- 违反：使用保留字
```

? **正确做法**：避免使用保留字

---

## 四、配置禁止项

### 4.1 检查点配置禁止

? **禁止不配置检查点**

```sql
-- ? 错误示例
-- 缺少检查点配置
```

? **正确做法**：必须配置检查点间隔

---

? **禁止使用长检查点间隔**

```sql
-- ? 错误示例
SET 'execution.checkpointing.interval' = '1h';  -- 违反：间隔太长
```

? **正确做法**：检查点间隔不超过 10 分钟

---

### 4.2 状态配置禁止

? **禁止不配置状态 TTL**

```sql
-- ? 错误示例
-- 状态无 TTL，可能无限增长
```

? **正确做法**：必须配置状态 TTL

---

## 五、禁止汇总速查表

### 5.1 分层禁止项

| 层级 | 禁止项 |
|-----|--------|
| ODS | 重度业务聚合、跳过 ODS 直连 DIM、不注入元数据字段、丢弃源字段 |
| DIM | 主键不一致、做事实聚合、重复维度属性 |
| DWD | 多业务过程混表、使用处理时间、跳过维度关联 |
| DWS | 粒度不清、派生指标在 DWS 层定义 |
| ADS | 重复定义 DWS 指标、缺少解释字段 |

### 5.2 技术禁止项

| 类型 | 禁止项 |
|-----|--------|
| 字段类型 | TIMESTAMP(0)、DATETIME、FLOAT/DOUBLE |
| WATERMARK | 处理时间、不声明 |
| 分区 | 分区字段参与计算、基于入湖时间分区 |
| 命名 | 中文表名、缺少分层前缀、混用大小写 |
| 配置 | 不配置检查点、不配置状态 TTL |

---

## 六、豁免条款

### 6.1 豁免申请

以下情况可申请豁免，但必须在代码中明确注释说明：

1. 遗留系统兼容需求
2. 特殊业务场景规范无法覆盖
3. 性能优化必要突破

### 6.2 豁免注释格式

```sql
-- ?? 豁免：历史遗留系统兼容
-- 原因：XXX 系统表名规范与本项目不同
-- 申请人：张三
-- 申请时间：2025-12-24
SELECT ...
```

---

## 七、后续文档索引

| 文档 | 定位 |
|-----|------|
| 00-meta.md | AI 行为总纲 |
| 03-layer-modeling.md | 分层建模规范 |
| 04-field.md | 字段与数据类型规范 |

---

> 本文档列举了实时湖仓开发中必须避免的错误行为，是代码审查和自检的核心参考。
