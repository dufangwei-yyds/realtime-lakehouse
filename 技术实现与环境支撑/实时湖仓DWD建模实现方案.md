# 实时湖仓DWD建模实现方案

> 以下为对应 SQL 脚本的完整内容，已原样搬运，便于在仅支持 Markdown 的设备中查看。
> 源文件：实时湖仓DWD建模实现方案.sql

```sql
/********************************************************************************
 实时湖仓DWD建模解读
 明细宽表化(单表即分析)、事件时间语义、清洗、转换与统一 | 实时去重、Lookup Join & Interval Join/Window Join(关联)
 数据膨胀、关联不上/关联为空、数据延迟、不可重算、Lookup Join & Interval Join/Window Join(使用条件)

 在实时湖仓的构建中, DWD(Data Warehouse Detail,明细事实层)是整个架构的"腰部", 起着承上启下的关键作用
 它将ODS层的原始数据与DIM层的维度属性进行"化学反应", 生成一张张业务可直接理解、分析的明细宽表

 DWD层生产级建模规范: 三大核心原则
 1. 数据原子性与业务打平(Denormalization)
    DWD层不再是简单的同步, 而是"明细宽表化"
    原则: 将事实流(如订单、点击)与维度表(如商品、门店)进行关联, 将常用的维度属性(颜色、尺码、波段、区域)直接冗余到事实明细中
    目的: 下游DWS层和应用层查询时无需再Join维表, 实现"单表即分析"
 2. 事件时间语义(Event Time)
    规范: DWD层必须严格基于事件时间(Event Time)进行处理, 而非系统处理时间
    实践: 所有DWD表必须包含event_time字段, 且严格按此字段进行分区和水位线管理, 确保大促期间数据迟到时,统计逻辑依然准确
 3. 清洗、转换与统一(Standardization)
    规范: 统一全渠道的度量口径
    实践: 例如, 将来自POS、App、小程序的订单状态统一映射为标准的“待支付、已支付、已发货”; 将金额字段统一为DECIMAL(18, 2)
 4. 业务过程建模
    按照具体的业务动作(如下单、支付、退款)拆分, 每张表代表一个最细粒度的业务事实

 工业级建模方法: 实时关联与状态管理
 在DWD层的构建中, 最核心的技术动作是Join(关联)
 1. 事实流关联维度表(Lookup Join)
    方法: 使用Flink的FOR SYSTEM_TIME AS OF语法关联DIM层
    挑战: 维度数据可能更新
    标准: 必须开启Cache(缓存)策略(如lookup.cache.max-rows), 并权衡缓存失效时间, 保证关联到的是相对准确的维度
 2. 事实流关联事实流(Interval Join/Window Join)
    场景: 如"下单流"关联"支付流"
    标准: 必须定义时间区间(Interval), 例如订单产生后30分钟内必须匹配到支付消息, 否则视为未支付
 3. 实时去重(Exactly-once)
    方法: 利用Paimon的primary-key表结合deduplicate引擎
    规范: 即使ODS已经去重, DWD在关联维度后建议再次进行幂等性检查, 防止重试导致的明细膨胀

 DWD层技术选型: Paimon vs  Doris
 方案A: 全湖架构(Flink + Paimon) —— 推荐: 作为数据基石
 做法: DWD明细宽表依然存储在Paimon中
 优点:
    架构统一: ODS -> DWD -> DWS全链路在湖中, 易于维护和重跑
    写吞吐极高: Paimon的LSM-Tree结构能扛住双11级别的并发写入
    开放性: DWD 层数据不仅能给Doris用, 还能给Spark/Presto做离线分析
    缺点: 实时点查性能略逊于OLAP
 方案B: 湖仓一体/仓内计算(Flink + Doris) —— 推荐: 作为加速方案
 做法: DWD直接构建在Doris的Unique或Duplicate模型表中
 优点:
   查询极速: Doris索引丰富, DWD层的即席查询响应最快
   简化链路: 如果业务只用Doris, 可以减少一层湖的存储
   缺点: 高频更新和大规模Join对Doris集群压力较大, 成本通常高于湖
 工业级最佳实践：
 Flink + Paimon (DWD) ──▶ Doris(DWS/异步物化视图)
   原因: 在Paimon中构建DWD, 保证了数据的"原始、持久、可重算"; 然后将DWD挂载为Doris的外表, 或者通过Doris的异步物化视图进行加速
   价值: 既拥有了湖的低成本和灵活性, 又拥有了库的极致查询速度
 ********************************************************************************/

 /********************************************************************************
 * 整体说明
 * 1. 业务背景
 * 用户点击商品(事实流A)并在30分钟内完成下单(事实流B),随后实时关联我们之前做好的商品全维度宽表(DIM层),最终产出DWD层明细宽表
 * 事实流1(点击流): Kafka fashion_app_click
 * 事实流2(订单流): Kafka topic_order
 * 双流关联: 使用Interval Join,只有点击后30分钟内下的单才算作"推荐转化"
 * 维度关联: 使用Lookup Join关联Paimon中的dim_sku_wide
 * 去重: 使用Paimon Primary Key表实现Exactly-once

 * 2. 环境与源表准备(MySQL & Kafka & Paimon)
 * 2.1 维度表(已有): Append表：存储原始变更记录，仅追加不更新;History表：基于Append表生成SCD2格式，存储维度全量历史版本
 *                 Latest镜像表：仅存储维度最新状态，用于高效JOIN; 退化宽表：将多维度表物理打平，减少查询JOIN开销
 * 维度退化宽表: paimon_catalog.ods.dim_sku_wide(通过dim_base_latest、dim_attr_latest、dim_price_latest物理打平)
 *             dim_base_latest、dim_attr_latest、dim_price_latest
 *             dim_base_append、dim_attr_append、dim_price_append
 *             dim_base_history、dim_attr_history、dim_price_history
 *             mysql_goods_base、mysql_goods_attr、mysql_goods_price
 * 2.2 事实流(已有)
 * 点击流源表(已有)   paimon_catalog.ods.kafka_app_click_source
 * 订单流源表(已有)   paimon_catalog.ods.kafka_order_source
 * 点击流ODS表(已有)  paimon_catalog.ods.ods_kafka_app_click、
 *                  paimon_catalog.ods.kafka_app_click_dedup(去重表)
 *                  paimon_catalog.ods.ods_app_click_analyze(JSON解析表)
 * 订单流ODS表(已有)  paimon_catalog.ods.ods_kafka_order
 * ODS层写入逻辑(已实现)
 ********************************************************************************/

/********************************************************************************
 步骤1: Kafka生产级模拟数据准备
 * 为了保证Interval Join成功,订单流的item_id(sku_id)、uid必须与点击流一致,且时间差在30分钟内
 * 为了保证Lookup Join成功, 订单流的item_id(sku_id)、点击流的item_id(sku_id)必须与维度表一致

 -- 模拟发送到Kafka topic: fashion_app_click (点击流)
 {"uid":"U9886","item_id":"101","action":"click","event_ts":"2025-12-25 15:47:01","ext_info":"{\"promo_id\":\"5\",\"client_type\":\"Android\",\"page\":\"home\",\"spm\":\"x.y.z\"}"}
 {"uid":"U9888","item_id":"102","action":"click","event_ts":"2025-12-25 18:42:30","ext_info":"{\"promo_id\":\"7\",\"client_type\":\"Android\",\"page\":\"home\",\"spm\":\"x.y.z\"}"}

 -- 模拟发送到Kafka topic: fashion_app_order (订单流)
 {"oid":"ORD_20251224_002","uid":"U9886","item_id":"101","amt":299.00,"order_ts":"2025-12-25 15:19:01"}
 {"oid":"ORD_20251224_003","uid":"U9888","item_id":"102","amt":599.00,"order_ts":"2025-12-25 18:42:30"}
 ********************************************************************************/

/********************************************************************************
 * 步骤2: Paimon DWD层建模(Flink SQL)
 * 持久与重算: DWD明细全部在Paimon中, 即便业务口径变了(比如要增加"颜色"维度), 只需在Doris中修改物化视图定义,
 * 重新从Paimon跑一遍即可, 无需动实时流任务
 ********************************************************************************/

 /*
 * 步骤2.1: 创建Paimon Catalog及DWD数据库
 * 说明: 关联Paimon湖仓, DWD层存储明细宽表数据
 */
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',                                            -- 连接器类型: Paimon
    'warehouse' = 'hdfs://192.168.63.128:8020/paimon/fashion_dw'  -- Paimon仓库根路径
);
USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS dwd;                                -- 创建ODS层数据库
USE dwd;

/*
 * 步骤2.2: 创建DWD明细宽表(原子性、去重、标准化)
 * 遵循"三大原则",定义主键以实现实时去重
 */
CREATE TABLE paimon_catalog.dwd.dwd_sales_detail (
    order_id STRING,
    user_id STRING,
    sku_id INT,-- 注意: 需与dim_sku_wide的类型INT保持一致
    -- 退化维度字段
    sku_name STRING,
    tag_price DECIMAL(10,2),
    season STRING,
    -- 事实度量
    order_amount DECIMAL(10,2),
    -- 时间属性 (标准化)
    click_ts TIMESTAMP(3),
    order_ts TIMESTAMP(3),
    _ingest_time TIMESTAMP(3),
    _order_kafka_offset BIGINT,
    _order_kafka_partition INT,
    PRIMARY KEY (order_id) NOT ENFORCED -- 实时去重(Exactly-once)
) WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    -- 优化: 开启Lookup缓存,减少对Paimon维表的访问压力,加速关联维表时的性能
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1h'
);

/********************************************************************************
 * 步骤3: Flink SQL实时清洗与摄入
 * 核心ETL逻辑: 双流Interval Join+维表Lookup Join, 支持流批一体重算
 ********************************************************************************/
SET 'table.exec.state.ttl' = '45 min'; -- 状态保留略大于Join窗口(30min)
SET 'execution.runtime-mode' = 'streaming';  -- 流模式执行
SET 'execution.checkpointing.interval' = '10s';  -- 检查点间隔(必配)
SET 'table.exec.sink.upsert-materialize' = 'NONE';  -- 禁用Upsert物化,提升性能

INSERT INTO paimon_catalog.dwd.dwd_sales_detail -- 注意三张表的rowtime时间,只有当三张表都有新数据写入,才会触发上一条记录写入
SELECT
    o.order_id,
    o.user_id,
    CAST(o.sku_id AS INT) as sku_id,-- 类型转换对齐维表主键
    -- 获取维度属性
    s.sku_name,
    s.tag_price,
    s.season,
    -- 事实数据
    o.order_amount,
    c._event_time as click_ts,
    o._event_time as order_ts,
    -- 元数据注入
    CURRENT_TIMESTAMP as _ingest_time,
    o._kafka_offset as _order_kafka_offset,
    o._kafka_partition as _order_kafka_partition
FROM paimon_catalog.ods.ods_kafka_order AS o
-- 1. Interval Join 【事实流(订单)关联事实流(点击)】
-- 业务逻辑: 订单必须在点击后的[0,30分钟]内发生
LEFT JOIN paimon_catalog.ods.ods_kafka_app_click AS c ON o.user_id = c.user_id AND o.sku_id = c.sku_id
-- 2. Lookup Join (事实流关联维度退化宽表,做好的物理打平表dim_sku_wide)
-- 使用 FOR SYSTEM_TIME AS OF 关联Paimon维表最新快照
LEFT JOIN paimon_catalog.ods.dim_sku_wide FOR SYSTEM_TIME AS OF o._event_time AS s
ON CAST(o.sku_id AS INT) = s.sku_id
WHERE o._event_time BETWEEN c._event_time AND c._event_time + INTERVAL '30' MINUTE;

/********************************************************************************
 * 步骤4: 验证与查询示例
 ********************************************************************************/
/*
 * 验证1: 验证Exactly-once(实时去重)
 * 模拟方案: 故意向Kafka fashion_app_order发送两条order_id相同的消息
 * 预期: 由于Paimon DWD表定义了PRIMARY KEY (order_id),湖仓中只会保留一条最新的记录,避免了重复计算

 * 验证2: 验证Interval Join(迟到容错)
 * 模拟方案: 先发点击消息,过了5分钟再发订单消息。
 * 预期: Flink 会在状态中保留点击流,等订单流一到,自动撮合关联。若超过 30 分钟下单,则 click_ts 为 NULL
 */

```

---

## 基于已验证 DIM 与 ODS 的生产版 DWD 层建设

> 说明：以下内容基于当前已经验证完成的两份文档继续向下建设：
>
> - [实时湖仓维度建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓维度建模实现方案.md)
> - [实时湖仓事件流建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓事件流建模实现方案.md)
>
> 核心依赖对象：
>
> - 维度宽表：`dim_sku_wide`
> - 订单事实：`ods_order_item_cdc`
> - 库存流水事实：`ods_inventory_txn_cdc`
> - 物流轨迹事实：`ods_api_logistics_track`、`ods_api_logistics_track_analyze`
> - 社媒事实：`ods_social_mention`、`ods_social_mention_analyze`
> - 用户行为事实：`ods_app_click_analyze`
>
> DWD 层统一落在 Paimon 中，后续通过 Doris 外部表方式访问。

## 一、DWD 建设目标

围绕两大核心场景沉淀可直接分析、可重算、可通过 Doris 外部表访问的 DWD 明细层：

1. 全渠道实时库存可视与智能调拨
2. 爆款趋势识别与快速追单

### 设计原则

- 单表即分析：核心维度属性直接冗余到事实明细中
- 事件时间优先：所有明细表都保留 `event_time`
- 统一编码口径：统一使用 `sku_id / spu_code / order_id`
- 幂等兜底：DWD 继续通过主键表防止重试膨胀
- 可外表访问：表结构避免过度嵌套，便于 Doris 外部表读取

## 二、源表与目标表映射

| 场景           | ODS 输入                                           | DWD 输出                        | 用途               |
| -------------- | -------------------------------------------------- | ------------------------------- | ------------------ |
| 库存可视与调拨 | `ods_inventory_txn_cdc` + `dim_sku_wide`           | `dwd_inventory_flow_detail_rt`  | 库存流水宽表       |
| 库存可视与调拨 | `ods_order_item_cdc` + `dim_sku_wide`              | `dwd_order_trade_detail_rt`     | 订单交易明细宽表   |
| 库存可视与调拨 | `ods_api_logistics_track_analyze` + `dim_sku_wide` | `dwd_logistics_track_detail_rt` | 物流轨迹宽表       |
| 爆款趋势识别   | `ods_app_click_analyze` + `dim_sku_wide`           | `dwd_user_behavior_detail_rt`   | 用户行为宽表       |
| 爆款趋势识别   | `ods_social_mention_analyze` + `dim_sku_wide`      | `dwd_social_mention_detail_rt`  | 社媒舆情宽表       |
| 爆款趋势识别   | 订单 + 行为 + 社媒 DWD                             | `dwd_hot_sku_event_detail_rt`   | 爆款事件归一化明细 |

## 三、公共准备

```sql
USE CATALOG paimon_catalog;

CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- SPU 级桥接视图：社媒数据部分场景只有 spu_code，没有 sku_id
CREATE TEMPORARY VIEW v_dim_spu_bridge AS
SELECT
    spu_code,
    MAX(brand_name) AS brand_name,
    MAX(product_year) AS product_year,
    MAX(season) AS season,
    MAX(wave_band) AS wave_band,
    MAX(series) AS series,
    MAX(product_type) AS product_type,
    MAX(qdtype_pdt) AS qdtype_pdt
FROM dim.dim_sku_wide
GROUP BY spu_code;
```

## 四、场景一：全渠道实时库存可视与智能调拨

> 正式版说明：为保证 DWS 层可直接基于 DWD 明细做 `TUMBLE / HOP / CUMULATE` 等事件时间窗口聚合，以下 DWD 表在建表时统一直接定义 `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`，不再依赖额外的 `_wm` 过渡表。

### 4.1 订单交易明细宽表

```sql
CREATE TABLE dwd_order_trade_detail_rt (
    order_id STRING,
    line_no INT,
    user_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    series STRING,
    product_type STRING,
    qdtype_pdt STRING,
    color_code STRING,
    size_code STRING,
    channel_code STRING,
    store_id STRING,
    order_status STRING,
    qty DECIMAL(12, 2),
    sale_price DECIMAL(12, 2),
    sale_amount DECIMAL(18, 2),
    order_time TIMESTAMP(3),
    pay_time TIMESTAMP(3),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    ingest_time TIMESTAMP(3),
    partition_dt STRING,
    PRIMARY KEY (order_id, line_no, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'ingest_time',
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1 h'
);

INSERT INTO dwd_order_trade_detail_rt
SELECT
    o.order_id,
    o.line_no,
    o.user_id,
    o.sku_id,
    d.spu_code,
    d.sku_name,
    d.brand_name,
    d.season,
    d.wave_band,
    d.series,
    d.product_type,
    d.qdtype_pdt,
    d.color_code,
    d.size_code,
    o.channel_code,
    o.store_id,
    o.order_status,
    o.qty,
    o.sale_price,
    o.sale_amount,
    o.order_time,
    o.pay_time,
    o._event_time AS event_time,
    o._ingest_time AS ingest_time,
    o._partition_dt AS partition_dt
FROM ods.ods_order_item_cdc AS o
LEFT JOIN dim.dim_sku_wide AS d
ON o.sku_id = d.sku_id;
```

### 4.2 库存流水明细宽表

```sql
CREATE TABLE dwd_inventory_flow_detail_rt (
    txn_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    product_type STRING,
    qdtype_pdt STRING,
    color_code STRING,
    size_code STRING,
    store_id STRING,
    warehouse_id STRING,
    biz_type STRING,
    biz_no STRING,
    qty DECIMAL(12, 2),
    stock_status STRING,
    biz_time TIMESTAMP(3),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    ingest_time TIMESTAMP(3),
    partition_dt STRING,
    PRIMARY KEY (txn_id,partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'ingest_time',
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1 h'
);

INSERT INTO dwd_inventory_flow_detail_rt
SELECT
    i.txn_id,
    i.sku_id,
    d.spu_code,
    d.sku_name,
    d.brand_name,
    d.season,
    d.wave_band,
    d.product_type,
    d.qdtype_pdt,
    d.color_code,
    d.size_code,
    i.store_id,
    i.warehouse_id,
    i.biz_type,
    i.biz_no,
    i.qty,
    i.stock_status,
    i.biz_time,
    i._event_time AS event_time,
    i._ingest_time AS ingest_time,
    i._partition_dt AS partition_dt
FROM ods.ods_inventory_txn_cdc AS i
LEFT JOIN dim.dim_sku_wide AS d
ON i.sku_id = d.sku_id;
```

### 4.3 物流轨迹明细宽表

```sql
CREATE TABLE dwd_logistics_track_detail_rt (
    logistics_no STRING,
    order_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    product_type STRING,
    carrier_code STRING,
    node_code STRING,
    node_name STRING,
    province STRING,
    city STRING,
    station_code STRING,
    courier_name STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    ingest_time TIMESTAMP(3),
    partition_dt STRING,
    partition_hh STRING,
    PRIMARY KEY (logistics_no, node_code, event_time, partition_dt, partition_hh) NOT ENFORCED
) PARTITIONED BY (partition_dt, partition_hh)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'ingest_time',
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1 h'
);

INSERT INTO dwd_logistics_track_detail_rt
SELECT
    l.logistics_no,
    l.order_id,
    l.sku_id,
    d.spu_code,
    d.sku_name,
    d.brand_name,
    d.season,
    d.product_type,
    l.carrier_code,
    l.node_code,
    l.node_name,
    l.province,
    l.city,
    l.station_code,
    l.courier_name,
    l._event_time AS event_time,
    CURRENT_TIMESTAMP AS ingest_time,
    DATE_FORMAT(l._event_time, 'yyyyMMdd') AS partition_dt,
    DATE_FORMAT(l._event_time, 'HH') AS partition_hh
FROM ods.ods_api_logistics_track_analyze AS l
LEFT JOIN dim.dim_sku_wide AS d
ON l.sku_id = d.sku_id;
```

## 五、场景二：爆款趋势识别与快速追单

### 5.1 用户行为明细宽表

```sql
CREATE TABLE dwd_user_behavior_detail_rt (
    user_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    series STRING,
    product_type STRING,
    qdtype_pdt STRING,
    behavior STRING,
    device_type STRING,
    page_id STRING,
    promotion_id STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    json_parse_status STRING,
    partition_dt STRING,
    partition_hh STRING,
    PRIMARY KEY (user_id, sku_id, behavior, event_time, partition_dt, partition_hh) NOT ENFORCED
) PARTITIONED BY (partition_dt, partition_hh)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'event_time',
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1 h'
);

INSERT INTO dwd_user_behavior_detail_rt
SELECT
    b.user_id,
    b.sku_id,
    d.spu_code,
    d.sku_name,
    d.brand_name,
    d.season,
    d.wave_band,
    d.series,
    d.product_type,
    d.qdtype_pdt,
    b.behavior,
    b.device_type,
    b.page_id,
    b.promotion_id,
    b._event_time AS event_time,
    b.json_parse_status,
    DATE_FORMAT(b._event_time, 'yyyyMMdd') AS partition_dt,
    DATE_FORMAT(b._event_time, 'HH') AS partition_hh
FROM ods.ods_app_click_analyze AS b
LEFT JOIN dim.dim_sku_wide AS d
ON b.sku_id = d.sku_id
WHERE b.sku_id IS NOT NULL AND b.sku_id <> '-1';
```

### 5.2 社媒舆情明细宽表

```sql
CREATE TABLE dwd_social_mention_detail_rt (
    platform STRING,
    note_id STRING,
    author_id STRING,
    sku_id STRING,
    spu_code STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    series STRING,
    product_type STRING,
    keyword STRING,
    scene_tag STRING,
    creator_level STRING,
    is_video BOOLEAN,
    device_type STRING,
    like_cnt BIGINT,
    fav_cnt BIGINT,
    comment_cnt BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    json_parse_status STRING,
    partition_dt STRING,
    partition_hh STRING,
    PRIMARY KEY (platform, note_id, event_time, partition_dt, partition_hh) NOT ENFORCED
) PARTITIONED BY (partition_dt, partition_hh)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'event_time',
    'lookup.cache' = 'true',
    'lookup.cache-rows' = '10000',
    'lookup.cache-ttl' = '1 h'
);

-- 1. 尝试增加以下两个参数，强制优化器简化路径
SET 'table.optimizer.join-reorder-enabled' = 'false';
SET 'table.optimizer.common-subplan-reuse-enabled' = 'true';

-- 2. 使用 CTE 改写，替代 TEMPORARY VIEW
INSERT INTO dwd_social_mention_detail_rt
WITH spu_agg AS (
    -- 将原视图逻辑直接放入 CTE，减少 Planner 对视图元数据的解析开销
    SELECT
        spu_code,
        MAX(brand_name) AS brand_name,
        MAX(season) AS season,
        MAX(wave_band) AS wave_band,
        MAX(series) AS series,
        MAX(product_type) AS product_type
    FROM dim.dim_sku_wide
    GROUP BY spu_code
)
SELECT
    s.platform,
    s.note_id,
    s.author_id,
    s.sku_id,
    -- 优化 COALESCE 逻辑，优先取维度表的精确值
    COALESCE(d.spu_code, g.spu_code) AS spu_code,
    COALESCE(d.brand_name, g.brand_name) AS brand_name,
    COALESCE(d.season, g.season) AS season,
    COALESCE(d.wave_band, g.wave_band) AS wave_band,
    COALESCE(d.series, g.series) AS series,
    COALESCE(d.product_type, g.product_type) AS product_type,
    s.keyword,
    s.scene_tag,
    s.creator_level,
    s.is_video,
    s.device_type,
    CAST(s.like_cnt AS BIGINT),
    CAST(s.fav_cnt AS BIGINT),
    CAST(s.comment_cnt AS BIGINT),
    s._event_time AS event_time,
    s.json_parse_status,
    DATE_FORMAT(s._event_time, 'yyyyMMdd') AS partition_dt,
    DATE_FORMAT(s._event_time, 'HH') AS partition_hh
FROM ods.ods_social_mention_analyze AS s
LEFT JOIN dim.dim_sku_wide AS d
    ON s.sku_id = d.sku_id
LEFT JOIN spu_agg AS g
    ON s.spu_code = g.spu_code;
```

### 5.3 爆款事件归一化明细表

```sql
CREATE TABLE dwd_hot_sku_event_detail_rt (
    event_id STRING,
    event_source STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    series STRING,
    product_type STRING,
    metric_type STRING,
    metric_value DECIMAL(18, 2),
    user_id STRING,
    note_id STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    partition_dt STRING,
    partition_hh STRING,
    PRIMARY KEY (event_id, partition_dt, partition_hh) NOT ENFORCED
) PARTITIONED BY (partition_dt, partition_hh)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'event_time'
);

INSERT INTO dwd_hot_sku_event_detail_rt
SELECT
    CONCAT('ORD_', order_id, '_', CAST(line_no AS STRING)) AS event_id,
    'ORDER' AS event_source,
    sku_id,
    spu_code,
    sku_name,
    brand_name,
    season,
    wave_band,
    series,
    product_type,
    'SALE_AMOUNT' AS metric_type,
    CAST(sale_amount AS DECIMAL(18, 2)) AS metric_value,
    user_id,
    CAST(NULL AS STRING) AS note_id,
    event_time,
    partition_dt,
    '00' AS partition_hh
FROM dwd_order_trade_detail_rt

UNION ALL

SELECT
    CONCAT('ACT_', user_id, '_', sku_id, '_', DATE_FORMAT(event_time, 'yyyyMMddHHmmss')) AS event_id,
    'BEHAVIOR' AS event_source,
    sku_id,
    spu_code,
    sku_name,
    brand_name,
    season,
    wave_band,
    series,
    product_type,
    behavior AS metric_type,
    CAST(1 AS DECIMAL(18, 2)) AS metric_value,
    user_id,
    CAST(NULL AS STRING) AS note_id,
    event_time,
    partition_dt,
    partition_hh
FROM dwd_user_behavior_detail_rt

UNION ALL

SELECT
    CONCAT('SOC_', platform, '_', note_id) AS event_id,
    'SOCIAL' AS event_source,
    sku_id,
    spu_code,
    CAST(NULL AS STRING) AS sku_name,
    brand_name,
    season,
    wave_band,
    series,
    product_type,
    'SOCIAL_HOT' AS metric_type,
    CAST(like_cnt + fav_cnt + comment_cnt AS DECIMAL(18, 2)) AS metric_value,
    CAST(NULL AS STRING) AS user_id,
    note_id,
    event_time,
    partition_dt,
    partition_hh
FROM dwd_social_mention_detail_rt;
```

## 六、校验建议 SQL

```sql
-- 订单 DWD 去重校验
SELECT order_id, line_no, COUNT(*) AS cnt
FROM dwd_order_trade_detail_rt
GROUP BY order_id, line_no
HAVING COUNT(*) > 1;

-- 库存流水 DWD 去重校验
SELECT txn_id, COUNT(*) AS cnt
FROM dwd_inventory_flow_detail_rt
GROUP BY txn_id
HAVING COUNT(*) > 1;

-- 物流 DWD 分区校验
SELECT *
FROM dwd_logistics_track_detail_rt
WHERE DATE_FORMAT(event_time, 'yyyyMMdd') <> partition_dt
   OR DATE_FORMAT(event_time, 'HH') <> partition_hh;

-- 爆款事件统一表来源分布校验
SELECT event_source, metric_type, COUNT(*) AS cnt
FROM dwd_hot_sku_event_detail_rt
GROUP BY event_source, metric_type;
```

## 七、面向 Doris 外部表访问的建议

- DWD 表字段尽量扁平化，避免复杂嵌套结构
- 统一保留 `partition_dt / partition_hh`，便于 Doris 外部表过滤分区
- 主键表使用明确主键与 `sequence.field`，保证外表读取结果稳定
- 高频查询优先访问 DWD 明细宽表，再通过 Doris 物化视图做加速
