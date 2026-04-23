# Paimon 表设计模板

> 本模板用于快速创建符合规范的 Paimon 表结构

---

## 模板说明

本模板适用于创建以下类型的 Paimon 表：
- ODS 层事实表
- ODS 层维度表
- DIM 层维度表
- DWD 层明细表
- DWS 层汇总表

---

## 一、ODS 层 Append 表模板

```sql
/*
============================================================
表信息
============================================================
表名：ods_${source}_${table}
表类型：ODS 层 Append 表
说明：用于存储纯事件流数据，高吞吐追加模式
============================================================
*/

CREATE TABLE IF NOT EXISTS ods_${source}_${table} (
    -- ========== 业务核心字段 ==========
    user_id STRING COMMENT '用户ID',
    sku_id STRING COMMENT '商品SKU编码',
    behavior STRING COMMENT '用户行为',
    
    -- ========== 原始数据存根（应对Schema漂移）==========
    raw_json STRING COMMENT '原始JSON内容',
    
    -- ========== 元数据字段 ==========
    _event_time TIMESTAMP(3) COMMENT '业务事件时间',
    _ingest_time TIMESTAMP(3) COMMENT '入湖时间',
    _partition_dt STRING COMMENT '日期分区（yyyyMMdd）',
    _partition_hh STRING COMMENT '小时分区（HH）',
    
    -- ========== Kafka 元数据 ==========
    _kafka_offset BIGINT COMMENT 'Kafka偏移量',
    _kafka_partition INT COMMENT 'Kafka分区号',
    
    -- ========== WATERMARK ==========
    WATERMARK FOR _event_time AS _event_time - INTERVAL '5' SECOND
) COMMENT 'ODS层${table}事件流表'
PARTITIONED BY (_partition_dt, _partition_hh)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'append-only',
    'bucket' = '-1',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 二、ODS 层主键表模板

```sql
/*
============================================================
表信息
============================================================
表名：ods_${source}_${table}
表类型：ODS 层主键表
说明：用于存储CDC接入数据，支持去重合并
============================================================
*/

CREATE TABLE IF NOT EXISTS ods_${source}_${table} (
    -- ========== 业务核心字段 ==========
    order_id STRING COMMENT '订单号',
    line_no INT COMMENT '订单行号',
    sku_id STRING COMMENT '商品SKU编码',
    qty DECIMAL(12,2) COMMENT '数量',
    sale_amount DECIMAL(18,2) COMMENT '销售金额',
    
    -- ========== 元数据字段 ==========
    _event_time TIMESTAMP(3) COMMENT '业务事件时间',
    _ingest_time TIMESTAMP(3) COMMENT '入湖时间',
    _partition_dt STRING COMMENT '日期分区（yyyyMMdd）',
    
    -- ========== 主键声明（必须包含所有分区字段）==========
    PRIMARY KEY (order_id, line_no, _partition_dt) NOT ENFORCED
) COMMENT 'ODS层${table}CDC表'
PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'change-log-upsert',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'sequence.field' = '_ingest_time',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 三、DIM 层维度宽表模板

```sql
/*
============================================================
表信息
============================================================
表名：dim_${domain}_wide
表类型：DIM 层维度宽表
说明：统一维度口径，支持高频Join
============================================================
*/

CREATE TABLE IF NOT EXISTS dim_${domain}_wide (
    -- ========== 主键 ==========
    sku_id STRING COMMENT '商品SKU编码',
    
    -- ========== 维度属性 ==========
    spu_code STRING COMMENT '商品SPU编码',
    sku_name STRING COMMENT '商品名称',
    sku_short_name STRING COMMENT '商品简称',
    brand_id STRING COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    category_id STRING COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    season STRING COMMENT '季节',
    wave_band STRING COMMENT '波段',
    series STRING COMMENT '系列',
    product_type STRING COMMENT '产品类型',
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    unit STRING COMMENT '单位',
    cost_price DECIMAL(18,2) COMMENT '成本价',
    tag_price DECIMAL(18,2) COMMENT '吊牌价',
    
    -- ========== 状态字段 ==========
    sku_status STRING COMMENT '商品状态',
    channel_code STRING COMMENT '渠道编码',
    
    -- ========== 时间字段 ==========
    _event_time TIMESTAMP(3) COMMENT '变更时间',
    _ingest_time TIMESTAMP(3) COMMENT '入层时间',
    
    -- ========== 主键声明 ==========
    PRIMARY KEY (sku_id) NOT ENFORCED
) COMMENT 'DIM层商品维度宽表'
WITH (
    'connector' = 'paimon',
    'write-mode' = 'change-log-upsert',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 四、DWD 层明细表模板

```sql
/*
============================================================
表信息
============================================================
表名：dwd_${domain}_${process}_detail_rt
表类型：DWD 层明细表
说明：以事实过程为核心，关联维度属性
============================================================
*/

CREATE TABLE IF NOT EXISTS dwd_${domain}_${process}_detail_rt (
    -- ========== 事实主键 ==========
    order_id STRING COMMENT '订单号',
    line_no INT COMMENT '订单行号',
    
    -- ========== 业务字段 ==========
    sku_id STRING COMMENT '商品SKU编码',
    qty DECIMAL(12,2) COMMENT '购买数量',
    sale_price DECIMAL(18,2) COMMENT '销售单价',
    sale_amount DECIMAL(18,2) COMMENT '销售金额',
    discount_amount DECIMAL(18,2) COMMENT '优惠金额',
    pay_amount DECIMAL(18,2) COMMENT '支付金额',
    
    -- ========== 维度退化字段 ==========
    sku_name STRING COMMENT '商品名称',
    brand_name STRING COMMENT '品牌名称',
    category_name STRING COMMENT '分类名称',
    store_id STRING COMMENT '门店编码',
    store_name STRING COMMENT '门店名称',
    channel_code STRING COMMENT '渠道编码',
    
    -- ========== 状态字段 ==========
    order_status STRING COMMENT '订单状态',
    
    -- ========== 时间字段 ==========
    order_time TIMESTAMP(3) COMMENT '下单时间',
    pay_time TIMESTAMP(3) COMMENT '支付时间',
    
    -- ========== 元数据字段 ==========
    _event_time TIMESTAMP(3) COMMENT '业务事件时间',
    _ingest_time TIMESTAMP(3) COMMENT '入层时间',
    _partition_dt STRING COMMENT '日期分区（yyyyMMdd）',
    
    -- ========== 主键声明 ==========
    PRIMARY KEY (order_id, line_no, _partition_dt) NOT ENFORCED
) COMMENT 'DWD层${process}明细实时表'
PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'change-log-upsert',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'sequence.field' = '_ingest_time',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 五、DWS 层汇总表模板

```sql
/*
============================================================
表信息
============================================================
表名：dws_${theme}_${grain}_rt
表类型：DWS 层汇总表
说明：公共粒度定义，核心指标固化
============================================================
*/

CREATE TABLE IF NOT EXISTS dws_${theme}_${grain}_rt (
    -- ========== 粒度字段 ==========
    sku_id STRING COMMENT '商品SKU编码',
    store_id STRING COMMENT '门店编码',
    stat_date STRING COMMENT '统计日期（yyyyMMdd）',
    stat_hour STRING COMMENT '统计小时（HH）',
    
    -- ========== 维度退化字段 ==========
    sku_name STRING COMMENT '商品名称',
    brand_name STRING COMMENT '品牌名称',
    store_name STRING COMMENT '门店名称',
    channel_code STRING COMMENT '渠道编码',
    
    -- ========== 原子指标 ==========
    order_cnt BIGINT COMMENT '订单数',
    sale_qty DECIMAL(12,2) COMMENT '销售数量',
    sale_amt DECIMAL(18,2) COMMENT '销售金额',
    discount_amt DECIMAL(18,2) COMMENT '优惠金额',
    pay_amt DECIMAL(18,2) COMMENT '支付金额',
    uv_cnt BIGINT COMMENT '用户UV',
    
    -- ========== 衍生指标字段（可选）==========
    avg_price DECIMAL(18,2) COMMENT '平均单价',
    conversion_rate DECIMAL(10,4) COMMENT '转化率',
    
    -- ========== 元数据字段 ==========
    _event_time TIMESTAMP(3) COMMENT '统计周期起始时间',
    _ingest_time TIMESTAMP(3) COMMENT '入层时间',
    _partition_dt STRING COMMENT '日期分区（yyyyMMdd）',
    
    -- ========== 主键声明 ==========
    PRIMARY KEY (sku_id, store_id, stat_date, stat_hour, _partition_dt) NOT ENFORCED
) COMMENT 'DWS层${theme}${grain}汇总实时表'
PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'write-mode' = 'change-log-upsert',
    'merge-engine' = 'deduplicate',
    'bucket' = '4',
    'sequence.field' = '_ingest_time',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 六、SCD2 拉链表模板

```sql
/*
============================================================
表信息
============================================================
表名：dim_${entity}_history
表类型：SCD2 拉链表
说明：保留维度历史变化
============================================================
*/

CREATE TABLE IF NOT EXISTS dim_${entity}_history (
    -- ========== 主键 ==========
    sku_id STRING COMMENT '商品SKU编码',
    
    -- ========== 维度属性 ==========
    sku_name STRING COMMENT '商品名称',
    brand_name STRING COMMENT '品牌名称',
    price DECIMAL(18,2) COMMENT '价格',
    
    -- ========== SCD2 字段 ==========
    start_date STRING COMMENT '有效期开始（yyyyMMdd）',
    end_date STRING COMMENT '有效期结束（yyyyMMdd，99991231表示当前有效）',
    
    -- ========== 元数据字段 ==========
    _event_time TIMESTAMP(3) COMMENT '变更时间',
    
    -- ========== 主键声明 ==========
    PRIMARY KEY (sku_id, start_date) NOT ENFORCED
) COMMENT 'DIM层${entity}SCD2历史表'
WITH (
    'connector' = 'paimon',
    'merge-engine' = 'aggregate',
    'fields.sku_name.aggregate-function' = 'last_non_null_value',
    'fields.brand_name.aggregate-function' = 'last_non_null_value',
    'fields.price.aggregate-function' = 'last_non_null_value',
    'fields.end_date.aggregate-function' = 'max',
    'bucket' = '4',
    'file.format' = 'orc',
    'file.compression' = 'ZSTD'
);
```

---

## 七、表设计检查清单

### 7.1 命名检查

- [ ] 表名符合分层命名规范（ods_/dim_/dwd_/dws_）
- [ ] 表名全小写，无特殊字符
- [ ] 语义清晰，能表达业务含义

### 7.2 字段检查

- [ ] 时间字段使用 TIMESTAMP(3)
- [ ] 金额字段使用 DECIMAL(18,2)
- [ ] 数量字段使用 DECIMAL(12,2)
- [ ] 分区字段使用 STRING
- [ ] 所有字段有注释

### 7.3 主键检查

- [ ] 主键声明正确
- [ ] 主键包含所有分区字段
- [ ] 主键字段不为空

### 7.4 存储配置检查

- [ ] Merge Engine 选择正确
- [ ] Bucket 配置合理
- [ ] 文件格式正确（ORC）
- [ ] 压缩方式正确（ZSTD）

### 7.5 元数据检查

- [ ] _event_time 字段存在
- [ ] _ingest_time 字段存在（ODS/DWD/DWS）
- [ ] _partition_dt 字段存在
- [ ] WATERMARK 已声明（Append 表）

---

## 八、模板版本

| 版本 | 日期 | 变更说明 |
|-----|------|---------|
| v1.0 | 2026-04 | 初版发布 |
