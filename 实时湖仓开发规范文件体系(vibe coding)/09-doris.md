# Doris 开发规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Doris 表设计、外部表配置、物化视图规范  
> 适用范围：Doris ADS 层开发、外部表配置、查询优化

---

## 一、Doris 表类型

### 1.1 表类型说明

| 表类型 | 说明 | 适用场景 |
|-------|------|---------|
| Doris 内部表 | Doris 原生存储 | 高频查询、Dashboard |
| Doris 外部表 | 外部数据源 | 读取 Paimon、MySQL 等 |
| 物化视图 | 预计算结果 | 加速高频查询 |

### 1.2 表引擎说明

| 引擎类型 | 说明 | 适用场景 |
|---------|------|---------|
| OLAP | 聚合引擎 | 高频聚合查询 |
| JDBC | JDBC 引擎 | 读取外部 JDBC 数据源 |
| MySQL | MySQL 引擎 | 读取 MySQL 数据 |
| ELASTICSEARCH | ES 引擎 | 读取 ES 数据 |

---

## 二、内部表设计规范

### 2.1 表结构设计

```sql
-- ADS 结果表设计
CREATE TABLE ads_inventory_dashboard (
    sku_id VARCHAR(64) NOT NULL COMMENT '商品SKU编码',
    sku_name VARCHAR(255) COMMENT '商品名称',
    brand_name VARCHAR(64) COMMENT '品牌名称',
    store_id VARCHAR(32) COMMENT '门店编码',
    store_name VARCHAR(128) COMMENT '门店名称',
    available_qty DECIMAL(12,2) COMMENT '可用库存',
    locked_qty DECIMAL(12,2) COMMENT '锁定库存',
    atp_qty DECIMAL(12,2) COMMENT '可承诺库存',
    sale_amt_dd DECIMAL(18,2) COMMENT '当日销售额',
    sale_cnt_dd BIGINT COMMENT '当日销售数量',
    partition_dt VARCHAR(8) COMMENT '分区日期',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 解释字段
    atp_level VARCHAR(32) COMMENT 'ATP等级：HIGH/MEDIUM/LOW',
    atp_suggestion VARCHAR(255) COMMENT '补货建议',
    
    -- 主键
    PRIMARY KEY (sku_id, store_id, partition_dt)
)
ENGINE=OLAP
AGGREGATE KEY (sku_id, sku_name, brand_name, store_id, store_name, partition_dt)
COMMENT '库存驾驶舱结果表'
DISTRIBUTED BY HASH(sku_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",          -- 测试环境单副本
    "storage_medium" = "HDD",
    "auto-increment-offset" = "1"
);
```

### 2.2 聚合模型选择

| 聚合模型 | 说明 | 适用场景 |
|---------|------|---------|
| AGGREGATE | 自动聚合 | 指标预先聚合 |
| UNIQUE | 唯一模型 | 主键唯一 |
| DUPLICATE | 明细模型 | 保留明细 |

---

## 三、外部表开发规范

### 3.1 JDBC Resource 配置

```sql
-- 创建 Paimon JDBC Resource
CREATE RESOURCE IF NOT EXISTS paimon_resource
PROPERTIES (
    'type' = 'jdbc',
    'user' = '',
    'password' = '',
    'jdbc-url' = 'jdbc:mysql://localhost:9030',
    'driver-url' = 'mysql-connector-java-8.0.20.jar',
    'checksum' = '737bb4fc3fd0634d32b92eva7a1c4b7c',
    'resource-name' = 'paimon_resource'
);
```

### 3.2 Paimon 外部表

```sql
-- 读取 Paimon DWD 表
CREATE TABLE ads_dwd_order_ext (
    order_id VARCHAR(64) NOT NULL COMMENT '订单号',
    sku_id VARCHAR(64) COMMENT '商品SKU',
    sku_name VARCHAR(255) COMMENT '商品名称',
    brand_name VARCHAR(64) COMMENT '品牌名称',
    qty DECIMAL(12,2) COMMENT '数量',
    sale_amount DECIMAL(18,2) COMMENT '销售金额',
    order_time DATETIME COMMENT '下单时间',
    _event_time DATETIME COMMENT '事件时间'
)
ENGINE=JDBC
COMMENT 'DWD订单明细外部表'
PROPERTIES (
    'resource' = 'paimon_resource',
    'table' = 'dwd.dwd_order_trade_detail_rt'
);
```

### 3.3 外部表使用限制

?? **重要限制**

- 外部表仅支持读取，不支持写入
- 外部表查询性能较低，不适合大表查询
- 建议用于小表 JOIN 或数据同步

---

## 四、物化视图规范

### 4.1 物化视图适用场景

| 场景 | 说明 |
|-----|------|
| 高频聚合加速 | 订单汇总、商品统计 |
| 多表 JOIN 加速 | 宽表预构建 |
| 预计算复杂指标 | 转化率、留存率 |

### 4.2 物化视图创建

```sql
-- 创建物化视图加速日销售汇总
CREATE MATERIALIZED VIEW ads_sales_daily_mv
AS SELECT
    DATE_FORMAT(order_time, '%Y-%m-%d') AS sale_date,
    brand_name,
    COUNT(*) AS order_cnt,
    SUM(sale_amount) AS daily_sales,
    SUM(qty) AS daily_qty
FROM ads_dwd_order_ext
WHERE order_status = 'SIGNED'
GROUP BY
    DATE_FORMAT(order_time, '%Y-%m-%d'),
    brand_name;
```

### 4.3 物化视图刷新策略

```sql
-- 设置刷新策略
ALTER MATERIALIZED VIEW ads_sales_daily_mv
REFRESH COMPLETE START WITH ('2025-01-01 00:00:00')
NEXT (STR_TO_DATE('02:00:00', '%H:%i:%s'));

-- 手动刷新
REFRESH MATERIALIZED VIEW ads_sales_daily_mv;
```

---

## 五、测试环境配置

### 5.1 单 BE 配置

?? **测试环境必须配置单副本**

```sql
-- 测试环境 ADS 表配置
PROPERTIES (
    "replication_num" = "1"    -- 单副本
);
```

### 5.2 生产环境配置

```sql
-- 生产环境 ADS 表配置
PROPERTIES (
    "replication_num" = "3"    -- 三副本
);
```

---

## 六、查询优化规范

### 6.1 分区裁剪

```sql
-- ? 正确：使用分区字段
SELECT * FROM ads_inventory_dashboard
WHERE partition_dt = '20251224';

-- ? 错误：不使用分区字段
SELECT * FROM ads_inventory_dashboard
WHERE update_time >= '2025-12-24';
```

### 6.2 Join 优化

```sql
-- ? 正确：小表 JOIN 大表
SELECT a.sku_id, a.sku_name, b.daily_sales
FROM dim_sku a
JOIN ads_sales_daily b ON a.sku_id = b.sku_id;

-- ?? 注意：避免大表 JOIN 大表
```

### 6.3 物化视图加速

```sql
-- 自动选择物化视图
SELECT sale_date, brand_name, daily_sales
FROM ads_sales_daily_mv  -- 直接查询物化视图
WHERE sale_date >= '2025-12-01';
```

---

## 七、Doris 开发自检清单

### 7.1 表设计检查

- [ ] 表名符合 ADS 层命名规范
- [ ] 字段类型正确
- [ ] 字段注释完整
- [ ] 解释字段已添加

### 7.2 外部表检查

- [ ] JDBC Resource 已配置
- [ ] 外部表连接正确
- [ ] 适用场景正确

### 7.3 物化视图检查

- [ ] 刷新策略已定义
- [ ] 加速场景明确

### 7.4 环境配置检查

- [ ] 测试环境 replication_num=1
- [ ] 生产环境 replication_num=3

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 08-plink-sql.md | Flink SQL 开发规范 |
| 10-quality.md | 数据质量与验证规范 |
| 11-deploy.md | 发布与回滚规范 |

---

> 本文档定义了 Doris ADS 层开发规范，是 Doris 表设计和查询优化的核心参考。
