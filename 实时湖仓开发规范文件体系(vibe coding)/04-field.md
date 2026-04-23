# 字段与数据类型规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：字段类型定义、主键规范、元数据字段规范、枚举值规范  
> 适用范围：ODS/DIM/DWD/DWS/ADS 全链路表结构设计、字段选型决策

---

## 一、字段类型体系

### 1.1 类型速查表

| 语义类型 | 数据类型 | 精度/长度 | 说明 | 使用示例 |
|---------|---------|---------|------|---------|
| 主键 | STRING | - | 字符串类型主键 | sku_id, order_id |
| 业务时间 | TIMESTAMP(3) | 3位毫秒 | 事件发生时间 | order_time, biz_time |
| 入湖时间 | TIMESTAMP(3) | 3位毫秒 | 入湖/入层时间 | _ingest_time |
| 日期分区 | STRING | - | yyyyMMdd 格式 | _partition_dt |
| 小时分区 | STRING | - | HH 格式 | _partition_hh |
| 金额 | DECIMAL | (18,2) | 18位总精度，2位小数 | sale_amount |
| 数量 | DECIMAL | (12,2) | 12位总精度，2位小数 | qty, sale_qty |
| 状态 | STRING | - | 枚举状态 | order_status |
| 渠道编码 | STRING | - | 渠道标识 | channel_code |
| 比率/比例 | DECIMAL | (10,4) | 保留4位小数 | conversion_rate |
| 序号/行号 | INT/BIGINT | - | 序号 | line_no, row_num |

### 1.2 禁止使用的数据类型

? **以下数据类型严格禁止在实时湖仓中使用**

| 禁止类型 | 原因 | 替代方案 |
|---------|------|---------|
| TIMESTAMP(0) | 精度不足，无法处理毫秒级事件 | TIMESTAMP(3) |
| DATETIME | 与 Flink 时间语义不统一 | TIMESTAMP(3) |
| VARCHAR(n) | 金额/数量不可变长 | DECIMAL |
| FLOAT/DOUBLE | 精度问题，金额不可用 | DECIMAL |
| TINYINT/SMALLINT | 服装行业金额/数量可能超范围 | INT/BIGINT/DECIMAL |
| 无主键声明 | 事实表必须有主键 | STRING + PRIMARY KEY |

---

## 二、主键规范

### 2.1 主键设计原则

| 原则 | 说明 |
|-----|------|
| 必须声明 | 每张事实表必须明确声明主键 |
| 跨层一致 | 主键类型和语义必须在 ODS->DWD->DWS->ADS 全链路一致 |
| 业务语义 | 主键必须有明确业务含义 |
| 唯一性 | 主键值必须唯一 |

### 2.2 主键声明方式

**Flink CDC 源表**
```sql
CREATE TEMPORARY TABLE cdc_sale_order (
    order_id STRING,
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    ...
);
```

**Paimon 主键表**
```sql
CREATE TABLE ods_order_item_cdc (
    order_id STRING,
    line_no INT,
    ...
    PRIMARY KEY (order_id, line_no, _partition_dt) NOT ENFORCED
) PARTITIONED BY (_partition_dt)
WITH (
    'connector' = 'paimon',
    'merge-engine' = 'deduplicate'
);
```

### 2.3 主键与分区字段

?? **重要规则**

Paimon 主键表的主键必须包含所有分区字段：

```sql
-- ? 正确：主键包含分区字段
PRIMARY KEY (order_id, line_no, _partition_dt)

-- ? 错误：主键不包含分区字段
PRIMARY KEY (order_id, line_no)
```

---

## 三、元数据字段规范

### 3.1 ODS 层元数据字段

| 字段名 | 类型 | 含义 | 必须注入 |
|-------|------|------|---------|
| _event_time | TIMESTAMP(3) | 业务事件时间 | ? |
| _ingest_time | TIMESTAMP(3) | 入湖时间 | ? |
| _partition_dt | STRING | 日期分区（yyyyMMdd） | ? |
| _partition_hh | STRING | 小时分区（HH） | 行为流必须 |
| _kafka_offset | BIGINT | Kafka 偏移量 | Kafka 接入必须 |
| _kafka_partition | INT | Kafka 分区号 | Kafka 接入必须 |
| json_parse_status | STRING | JSON 解析状态 | JSON 解析表 |

### 3.2 DWD 层元数据字段

| 字段名 | 类型 | 含义 | 必须注入 |
|-------|------|------|---------|
| _event_time | TIMESTAMP(3) | 业务事件时间 | ? |
| _ingest_time | TIMESTAMP(3) | 入层时间 | ? |
| _partition_dt | STRING | 日期分区（yyyyMMdd） | ? |

### 3.3 DWS 层元数据字段

| 字段名 | 类型 | 含义 | 必须注入 |
|-------|------|------|---------|
| _event_time | TIMESTAMP(3) | 统计周期起始时间 | ? |
| _ingest_time | TIMESTAMP(3) | 入层时间 | ? |
| _partition_dt | STRING | 日期分区（yyyyMMdd） | ? |

### 3.4 元数据字段生成规则

**_event_time 生成规则**
```sql
-- CDC 接入：优先使用业务时间
COALESCE(order_time, updated_at) AS _event_time

-- Kafka 接入：从事件时间字段提取
event_ts AS _event_time

-- 无明确业务时间：从当前时间
CURRENT_TIMESTAMP AS _event_time  -- ?? 仅作为兜底
```

**_ingest_time 生成规则**
```sql
CURRENT_TIMESTAMP AS _ingest_time
```

**分区字段生成规则**
```sql
-- 日期分区
DATE_FORMAT(_event_time, 'yyyyMMdd') AS _partition_dt

-- 小时分区
DATE_FORMAT(_event_time, 'HH') AS _partition_hh
```

---

## 四、字段类型详细定义

### 4.1 业务主键字段

| 字段名 | 类型 | 业务含义 | 服装行业示例 |
|-------|------|---------|------------|
| sku_id | STRING | 商品 SKU 编码 | SKU001, SKU002 |
| spu_code | STRING | 商品 SPU 编码 | SPU001 |
| order_id | STRING | 订单号 | ORD202512230001 |
| txn_id | STRING | 库存流水号 | TXN202512230001 |
| user_id | STRING | 用户 ID | U9876 |
| store_id | STRING | 门店编码 | S001 |
| warehouse_id | STRING | 仓库编码 | W001 |
| logistics_no | STRING | 物流单号 | LN202512230001 |

### 4.2 金额与数量字段

**金额字段规范**
```sql
-- 订单金额
sale_amount DECIMAL(18,2)

-- 优惠金额
discount_amount DECIMAL(18,2)

-- 运费
freight_amount DECIMAL(18,2)

-- 支付金额
pay_amount DECIMAL(18,2)
```

**数量字段规范**
```sql
-- 购买数量
qty DECIMAL(12,2)

-- 库存变动数量（正负表示出入）
stock_qty DECIMAL(12,2)

-- 转化率（百分比）
conversion_rate DECIMAL(10,4)
```

### 4.3 时间字段

**业务时间字段**
```sql
-- 下单时间
order_time TIMESTAMP(3)

-- 支付时间
pay_time TIMESTAMP(3)

-- 业务发生时间
biz_time TIMESTAMP(3)

-- 发货时间
ship_time TIMESTAMP(3)

-- 签收时间
sign_time TIMESTAMP(3)
```

**元数据时间字段**
```sql
-- 事件时间（湖仓统一）
_event_time TIMESTAMP(3)

-- 入湖/入层时间
_ingest_time TIMESTAMP(3)
```

---

## 五、枚举值规范

### 5.1 订单状态 (order_status)

| 值 | 含义 | 说明 |
|---|------|------|
| CREATED | 已创建 | 订单已生成，待支付 |
| PAID | 已支付 | 完成支付，待发货 |
| SHIPPED | 已发货 | 已出库，进入物流 |
| SIGNED | 已签收 | 用户已收货 |
| CLOSED | 已关闭 | 超时未支付/取消/系统关闭 |
| REFUNDING | 退款中 | 发起退款流程 |
| REFUNDED | 已退款 | 完成退款 |

### 5.2 渠道编码 (channel_code)

| 值 | 含义 | 说明 |
|---|------|------|
| EC | 电商渠道 | 天猫、京东、官方商城 |
| STORE | 门店渠道 | 线下门店 POS |
| MINIAPP | 小程序 | 微信/支付宝小程序 |
| LIVE | 直播渠道 | 直播带货 |
| DISTRIBUTOR | 分销渠道 | 分销商、经销商 |

### 5.3 库存业务类型 (biz_type)

| 值 | 含义 | 说明 |
|---|------|------|
| PUR_IN | 采购入库 | 采购单到货 |
| SALE_OUT | 销售出库 | 订单扣减库存 |
| RET_IN | 退货入库 | 用户退货 |
| TRANSFER_OUT | 调拨出库 | 从当前仓/店调出 |
| TRANSFER_IN | 调拨入库 | 调拨到当前仓/店 |
| ALLOC_LOCK | 库存锁定 | 订单占用 |
| ALLOC_RELEASE | 锁定释放 | 取消订单释放 |
| ADJUST | 库存调整 | 盘点、报损、报溢 |

### 5.4 库存状态 (stock_status)

| 值 | 含义 | 说明 |
|---|------|------|
| AVAILABLE | 可用库存 | 可销售、可承诺 |
| LOCKED | 锁定库存 | 已被订单占用 |
| IN_TRANSIT | 在途库存 | 调拨运输中 |
| DEFECTIVE | 次品库存 | 不可直接销售 |
| FROZEN | 冻结库存 | 风控、争议冻结 |

### 5.5 用户行为 (behavior)

| 值 | 含义 | 说明 |
|---|------|------|
| CLICK | 点击 | 点击商品 |
| VIEW | 浏览 | 查看商品详情 |
| ADD_CART | 加购 | 加入购物车 |
| ORDER | 下单 | 提交订单 |
| PAY | 支付 | 完成支付 |
| LIKE | 点赞 | 点赞/收藏 |

---

## 六、JSON 字段处理规范

### 6.1 JSON 存储规范

**ODS 层 JSON 字段**
```sql
-- 保留原始 JSON
raw_json STRING

-- 扩展字段（Map 类型，便于查询）
ext_info MAP<STRING, STRING>
```

### 6.2 JSON 解析规范

```sql
-- 解析 JSON 字段（兼容 Schema 漂移）
JSON_VALUE(raw_json, '$.device') AS device_type
JSON_VALUE(raw_json, '$.page') AS page_id

-- 兼容新旧字段（Schema 漂移处理）
COALESCE(
    JSON_VALUE(raw_json, '$.client_type'),  -- 新字段
    JSON_VALUE(raw_json, '$.device')         -- 旧字段
) AS device_type
```

### 6.3 JSON 解析状态跟踪

```sql
-- 解析状态字段
CASE
    WHEN JSON_VALUE(raw_json, '$.client_type') IS NOT NULL THEN '解析成功（新字段）'
    WHEN JSON_VALUE(raw_json, '$.device') IS NOT NULL THEN '解析成功（旧字段）'
    ELSE '解析失败'
END AS json_parse_status
```

---

## 七、字段自检清单

### 7.1 所有字段检查

- [ ] 时间字段使用 TIMESTAMP(3)
- [ ] 金额字段使用 DECIMAL(18,2)
- [ ] 数量字段使用 DECIMAL(12,2)
- [ ] 主键字段类型跨层一致
- [ ] 枚举值使用标准化编码（大写下划线）

### 7.2 ODS 层额外检查

- [ ] 已注入 _event_time、_ingest_time
- [ ] 行为流已注入 _partition_hh
- [ ] Kafka 接入已注入 _kafka_offset、_kafka_partition
- [ ] JSON 解析表有 json_parse_status 字段

### 7.3 主键检查

- [ ] 每张事实表有明确的主键
- [ ] 主键包含所有分区字段
- [ ] 主键类型跨层一致

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 05-watermark-partition.md | 水位与分区规范 |
| 06-flink-sql.md | Flink SQL 开发规范 |
| 08-paimon.md | Paimon 存储规范 |

---

> 本文档定义了实时湖仓全链路字段类型规范，是表结构设计的核心参考。
