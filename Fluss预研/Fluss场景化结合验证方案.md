# Fluss场景化结合验证方案

> 文档定位：`Apache Fluss` 第三阶段 PoC OOTB 操作验证手册  
> 适用阶段：完成第二阶段最小链路 PoC 后，进入“与现有场景结合验证”阶段  
> 当前目标：在不改动一期主生产链路的前提下，使用增强测试数据方案，验证 `Fluss` 在更贴业务语义场景中的适配性

---

## 一、阶段目标

第三阶段不再停留在纯 `datagen` 的最小样例，而是引入更贴近业务语义的增强数据方案，重点验证：

1. `Fluss` 是否适合承载更贴业务的事件流与状态流
2. `Lookup Join` 在更真实数据语义下是否仍稳定可用
3. `PrimaryKey Table` 是否适合作为实时状态层或实时特征状态层候选
4. `Fluss` 是否值得进入下一步更小范围的业务试点评估

---

## 二、验证边界

### 2.1 本阶段纳入范围

- 场景 A 增强版：行为流 + 商品状态补全
- 场景 B 增强版：库存状态实时表
- 更业务化字段值
- 更可控的主键更新顺序
- 更可解释的结果校验

### 2.2 本阶段不纳入范围

- 一期正式作业迁移
- 现网数据回放
- 大规模性能压测
- 与 `Doris AI` 的联动实验

### 2.3 关于“是否纳入二期试点”的边界说明

这一点需要提前对齐：

- 本阶段只是为“是否建议纳入二期试点”提供依据
- 不代表当前就开始对一期项目做正式改造
- 一旦进入二期试点，才意味着要基于一期已经完成的生产级数据和实现，对项目进行小范围、可控、可回退的真实改造验证

因此本阶段输出的重点是：

- 场景适配性证据
- 对比结果
- 是否值得继续推进的建议

---

## 三、建议执行顺序

1. 创建独立的第三阶段实验库
2. 准备增强测试数据对象
3. 先执行一期当前技术栈的对照操作
4. 再执行场景 A 增强版
5. 再执行场景 B 增强版
6. 记录结果、问题与初步判断

---

## 四、公共准备

### 4.1 使用 Catalog

```sql
USE CATALOG fluss_catalog;
```

### 4.2 创建第三阶段独立实验库

```sql
CREATE DATABASE IF NOT EXISTS poc_fashion_stage3;
USE poc_fashion_stage3;
```

说明：

- 建议与第二阶段独立隔离，避免对象干扰

### 4.3 当前一期技术栈对照验证使用说明

为了给后续 [场景对比报告.md](/d:/workspace/realtime-lakehouse/场景对比报告.md) 做铺垫，本手册建议在执行 `Fluss` 场景化验证前，先按同一批增强数据，在一期现有技术栈上完成一轮对照操作。

本轮对照只做 OOTB 级验证，不改动一期已完成实现，重点观察：

- 用当前技术栈实现同样场景需要哪些对象
- SQL / 作业表达复杂度如何
- 结果校验是否清晰
- 与 `Fluss` 实现相比，理解成本和实施体验如何

本轮对照验证的统一要求如下：

- 直接切换到一期当前 `Paimon catalog`
- 使用与 `Fluss` 场景化验证一致的表结构字段
- 使用与 `Fluss` 场景化验证一致的增强测试数据
- 仅增加最小对照表，不改动一期已完成的正式对象

建议主要参考以下现有文档：

- [实时湖仓事件流建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓事件流建模实现方案.md)
- [实时湖仓维度建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓维度建模实现方案.md)
- [实时湖仓DWD建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓DWD建模实现方案.md)

### 4.4 一期当前技术栈公共准备

以下 SQL 作为两个对照场景的公共准备步骤，可直接执行。

#### 4.4.1 切换到一期 Paimon Catalog

如果当前会话尚未创建 `paimon_catalog`，请先按你一期环境的实际配置创建。  
若已存在，可直接 `USE CATALOG`。

```sql
USE CATALOG paimon_catalog;
```

#### 4.4.2 创建第三阶段对照实验库

```sql
CREATE DATABASE IF NOT EXISTS poc_fashion_stage3_compare;
USE poc_fashion_stage3_compare;
```

说明：

- 本实验库只用于第三阶段对照验证
- 不改动一期正式分层表
- 后续所有对照表均落在该库下

---

## 五、场景 A 增强版：行为流 + 商品状态补全

### 5.1 场景目标

本场景重点验证：

- 更业务化的行为事件是否适合进入 `Log Table`
- 商品状态主键表是否能稳定支撑 lookup 补全
- 富化结果是否更贴近真实业务理解

### 5.2 增强点

相比第二阶段，这一版增强点在于：

- `event_type` 使用明确业务枚举
- `brand_name`、`product_type`、`sku_name` 使用更贴业务的值
- 行为事件与商品状态之间的命中关系更清晰

### 5.2.1 一期当前技术栈 OOTB 对照操作

本场景对照目标：

- 使用 `Paimon append 表 + Paimon 主键表 + Flink Lookup Join`
- 在不改动一期正式对象的前提下，复刻与 `Fluss` 相同结构、相同测试数据、相同富化目标

#### Step 1：切换到对照实验库

```sql
USE CATALOG paimon_catalog;
USE poc_fashion_stage3_compare;
```

#### Step 2：创建行为事件 Paimon 表

```sql
CREATE TABLE paimon_behavior_event_log_s3 (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  partition_dt STRING
) PARTITIONED BY (partition_dt)
WITH (
  'connector' = 'paimon',
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'bucket-key' = 'sku_id'
);
```

说明：

- 这里虽然是 append 语义对照，但为了简化本地 PoC 建表与查询体验，仍使用 `Paimon` 表承载
- `partition_dt` 固定写入同一天，便于抽样查看

#### Step 3：创建商品状态 Paimon 主键表

```sql
CREATE TABLE paimon_sku_status_pk_s3 (
  sku_id BIGINT,
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT,
  status_time TIMESTAMP(3),
  partition_dt STRING,
  PRIMARY KEY (sku_id, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
  'connector' = 'paimon',
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'status_time'
);
```

#### Step 4：创建富化结果表

```sql
CREATE TABLE paimon_behavior_enriched_log_s3 (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT,
  partition_dt STRING
) PARTITIONED BY (partition_dt)
WITH (
  'connector' = 'paimon',
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'bucket-key' = 'sku_id'
);
```

#### Step 5：写入与 Fluss 完全一致的增强测试数据

说明：

- 这里直接使用 `INSERT INTO ... VALUES (...)`
- 不再先创建 `TEMPORARY VIEW` 再 `INSERT ... SELECT`
- 这是因为在当前 `Flink 1.20.x + Paimon` 组合下，`VALUES` 临时视图写入 `Paimon` 时可能触发 planner 异常
- 直接 `VALUES` 写入更稳妥，也更适合 OOTB 对照验证

行为事件写入：

```sql
INSERT INTO paimon_behavior_event_log_s3
VALUES
  (1, 10001, 1001, 'view',   TIMESTAMP '2026-04-07 10:00:01', '2026-04-07'),
  (2, 10002, 1001, 'click',  TIMESTAMP '2026-04-07 10:00:05', '2026-04-07'),
  (3, 10003, 1002, 'fav',    TIMESTAMP '2026-04-07 10:00:08', '2026-04-07'),
  (4, 10004, 1003, 'cart',   TIMESTAMP '2026-04-07 10:00:10', '2026-04-07'),
  (5, 10005, 1004, 'view',   TIMESTAMP '2026-04-07 10:00:12', '2026-04-07'),
  (6, 10006, 1005, 'click',  TIMESTAMP '2026-04-07 10:00:15', '2026-04-07'),
  (7, 10007, 1002, 'order',  TIMESTAMP '2026-04-07 10:00:18', '2026-04-07'),
  (8, 10008, 1003, 'view',   TIMESTAMP '2026-04-07 10:00:21', '2026-04-07'),
  (9, 10009, 1001, 'fav',    TIMESTAMP '2026-04-07 10:00:25', '2026-04-07'),
  (10, 10010, 1004, 'order', TIMESTAMP '2026-04-07 10:00:30', '2026-04-07');
```

商品状态写入：

```sql
INSERT INTO paimon_sku_status_pk_s3
VALUES
  (1001, '春季针织连衣裙', 'TBE',  '连衣裙',   1, TIMESTAMP '2026-04-07 09:59:50', '2026-04-07'),
  (1002, '轻薄防晒夹克',   'TBE',  '连帽夹克', 1, TIMESTAMP '2026-04-07 09:59:55', '2026-04-07'),
  (1003, '高腰直筒牛仔裤', 'LILY', '裤装',     1, TIMESTAMP '2026-04-07 10:00:00', '2026-04-07'),
  (1004, '法式碎花上衣',   'LILY', '上衣',     0, TIMESTAMP '2026-04-07 10:00:03', '2026-04-07'),
  (1005, '宽松短袖T恤',    'JNBY', 'T恤',      1, TIMESTAMP '2026-04-07 10:00:05', '2026-04-07');
```

#### Step 6：执行一期当前技术栈下的 Lookup/Join 富化

```sql
INSERT INTO paimon_behavior_enriched_log_s3
SELECT
  a.event_id,
  a.user_id,
  a.sku_id,
  a.event_type,
  a.event_time,
  b.sku_name,
  b.brand_name,
  b.product_type,
  b.available_flag,
  a.partition_dt
FROM (
  SELECT paimon_behavior_event_log_s3.*, PROCTIME() AS ptime
  FROM paimon_behavior_event_log_s3
) AS a
LEFT JOIN paimon_sku_status_pk_s3
FOR SYSTEM_TIME AS OF a.ptime AS b
ON a.sku_id = b.sku_id
AND a.partition_dt = b.partition_dt;
```

#### Step 7：查看对照结果

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM paimon_behavior_enriched_log_s3
LIMIT 50;
```

#### Step 8：记录对照观察项

重点记录：

- 一期当前技术栈下，为完成相同富化需要创建哪些表
- 对比 `Fluss`，建表和查询的表达复杂度如何
- 结果抽样核对是否清晰
- 哪一边更容易让人快速理解“事件流 + 状态流 + 富化”这件事

### 5.3 创建行为源表

为保证更可控，本阶段建议使用 `VALUES` 静态样例。

```sql
CREATE TEMPORARY VIEW v_behavior_events AS
SELECT *
FROM (
  VALUES
    (1, 10001, 1001, 'view',   TIMESTAMP '2026-04-07 10:00:01'),
    (2, 10002, 1001, 'click',  TIMESTAMP '2026-04-07 10:00:05'),
    (3, 10003, 1002, 'fav',    TIMESTAMP '2026-04-07 10:00:08'),
    (4, 10004, 1003, 'cart',   TIMESTAMP '2026-04-07 10:00:10'),
    (5, 10005, 1004, 'view',   TIMESTAMP '2026-04-07 10:00:12'),
    (6, 10006, 1005, 'click',  TIMESTAMP '2026-04-07 10:00:15'),
    (7, 10007, 1002, 'order',  TIMESTAMP '2026-04-07 10:00:18'),
    (8, 10008, 1003, 'view',   TIMESTAMP '2026-04-07 10:00:21'),
    (9, 10009, 1001, 'fav',    TIMESTAMP '2026-04-07 10:00:25'),
    (10, 10010, 1004, 'order', TIMESTAMP '2026-04-07 10:00:30')
) AS t(event_id, user_id, sku_id, event_type, event_time);
```

### 5.4 创建商品状态源表

```sql
CREATE TEMPORARY VIEW v_sku_status AS
SELECT *
FROM (
  VALUES
    (1001, '春季针织连衣裙', 'TBE',  '连衣裙', 1, TIMESTAMP '2026-04-07 09:59:50'),
    (1002, '轻薄防晒夹克',   'TBE',  '连衣夹克', 1, TIMESTAMP '2026-04-07 09:59:55'),
    (1003, '高腰直筒牛仔裤', 'LILY', '裤装', 1, TIMESTAMP '2026-04-07 10:00:00'),
    (1004, '法式碎花上衣',   'LILY', '上衣', 0, TIMESTAMP '2026-04-07 10:00:03'),
    (1005, '宽松短袖T恤',    'JNBY', 'T恤', 1, TIMESTAMP '2026-04-07 10:00:05')
) AS t(sku_id, sku_name, brand_name, product_type, available_flag, status_time);
```

### 5.5 创建行为 Log Table

```sql
CREATE TABLE fluss_behavior_event_log_s3 (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3)
) WITH (
  'bucket.num' = '4'
);
```

### 5.6 创建商品状态主键表

```sql
CREATE TABLE fluss_sku_status_pk_s3 (
  sku_id BIGINT,
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT,
  status_time TIMESTAMP(3),
  PRIMARY KEY (sku_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### 5.7 创建富化结果表

```sql
CREATE TABLE fluss_behavior_enriched_log_s3 (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT
) WITH (
  'bucket.num' = '4'
);
```

### 5.8 写入数据

```sql
INSERT INTO fluss_behavior_event_log_s3
SELECT *
FROM v_behavior_events;
```

```sql
INSERT INTO fluss_sku_status_pk_s3
SELECT *
FROM v_sku_status;
```

### 5.9 执行 Lookup Join

```sql
INSERT INTO fluss_behavior_enriched_log_s3
SELECT
  a.event_id,
  a.user_id,
  a.sku_id,
  a.event_type,
  a.event_time,
  b.sku_name,
  b.brand_name,
  b.product_type,
  b.available_flag
FROM (
  SELECT fluss_behavior_event_log_s3.*, PROCTIME() AS ptime
  FROM fluss_behavior_event_log_s3
) AS a
LEFT JOIN fluss_sku_status_pk_s3
FOR SYSTEM_TIME AS OF a.ptime AS b
ON a.sku_id = b.sku_id;
```

### 5.10 查看结果

建议优先使用 streaming 模式查看完整结果：

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_behavior_enriched_log_s3;
```

如需 batch 模式预览，请带 `LIMIT`：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_behavior_enriched_log_s3
LIMIT 50;
```

说明：

- 当前 `Fluss 0.8.0` 对 batch 查询存在边界限制
- 对 `Log Table` 或富化结果表做全表查询时，优先使用 `streaming`
- `batch` 更适合做有限结果预览

### 5.11 预期观察点

- `event_type` 是否体现真实业务动作
- `sku_name`、`brand_name`、`product_type` 是否正确补全
- `available_flag` 是否按商品状态正确命中
- 行为事件与商品状态之间的业务解释是否更自然
- 与一期现有技术栈实现相比，`Fluss` 是否更直接、更易理解

---

## 六、场景 B 增强版：库存状态实时表

### 6.1 场景目标

本场景重点验证：

- 使用更可控的状态更新顺序，观察 `PrimaryKey Table` 的最新态表现
- 判断 `Fluss` 是否适合作为库存状态层候选

### 6.2 增强点

- 不再完全随机生成库存状态
- 明确构造同主键多次更新
- 让状态变化更容易解释

### 6.2.1 一期当前技术栈 OOTB 对照操作

本场景对照目标：

- 使用 `Paimon` 主键表复刻与 `Fluss PrimaryKey Table` 相同的库存状态语义
- 使用完全一致的字段结构和完全一致的测试数据
- 对比两边对“最新态状态表”的表达体验

#### Step 1：切换到对照实验库

```sql
USE CATALOG paimon_catalog;
USE poc_fashion_stage3_compare;
```

#### Step 2：创建库存状态 Paimon 主键表

```sql
CREATE TABLE paimon_inventory_state_pk_s3 (
  sku_id BIGINT,
  store_id BIGINT,
  available_qty INT,
  locked_qty INT,
  in_transit_qty INT,
  update_time TIMESTAMP(3),
  partition_dt STRING,
  PRIMARY KEY (sku_id, store_id, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
  'connector' = 'paimon',
  'bucket' = '4',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'update_time'
);
```

#### Step 3：写入与 Fluss 完全一致的库存增强样例

说明：

- 同样采用 `INSERT INTO ... VALUES (...)`
- 避免 `VALUES` 临时视图写入 `Paimon` 时触发 planner 异常

```sql
INSERT INTO paimon_inventory_state_pk_s3
VALUES
  (1001, 1, 120, 10, 30, TIMESTAMP '2026-04-07 10:01:00', '2026-04-07'),
  (1001, 1, 118, 12, 28, TIMESTAMP '2026-04-07 10:01:10', '2026-04-07'),
  (1001, 1, 115, 15, 25, TIMESTAMP '2026-04-07 10:01:20', '2026-04-07'),
  (1002, 1,  80,  5, 12, TIMESTAMP '2026-04-07 10:01:00', '2026-04-07'),
  (1002, 1,  76,  8, 10, TIMESTAMP '2026-04-07 10:01:15', '2026-04-07'),
  (1003, 2,  66,  3, 20, TIMESTAMP '2026-04-07 10:01:00', '2026-04-07'),
  (1003, 2,  60,  6, 18, TIMESTAMP '2026-04-07 10:01:12', '2026-04-07'),
  (1004, 2,  20,  2,  5, TIMESTAMP '2026-04-07 10:01:00', '2026-04-07'),
  (1004, 2,  18,  4,  5, TIMESTAMP '2026-04-07 10:01:18', '2026-04-07'),
  (1005, 3, 150, 10, 40, TIMESTAMP '2026-04-07 10:01:00', '2026-04-07');
```

#### Step 4：查看对照结果

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM paimon_inventory_state_pk_s3
LIMIT 50;
```

#### Step 5：记录对照观察项

重点记录：

- 在一期当前技术栈下，为表达“库存最新态”需要哪些配置
- `sequence.field`、主键、分区设计是否增加理解成本
- 对比 `Fluss PrimaryKey Table`，哪一种更直观表达“状态表”
- 两边抽样结果是否都符合预期

### 6.3 创建库存状态源视图

```sql
CREATE TEMPORARY VIEW v_inventory_state AS
SELECT *
FROM (
  VALUES
    (1001, 1, 120, 10, 30, TIMESTAMP '2026-04-07 10:01:00'),
    (1001, 1, 118, 12, 28, TIMESTAMP '2026-04-07 10:01:10'),
    (1001, 1, 115, 15, 25, TIMESTAMP '2026-04-07 10:01:20'),
    (1002, 1, 80,  5,  12, TIMESTAMP '2026-04-07 10:01:00'),
    (1002, 1, 76,  8,  10, TIMESTAMP '2026-04-07 10:01:15'),
    (1003, 2, 66,  3,  20, TIMESTAMP '2026-04-07 10:01:00'),
    (1003, 2, 60,  6,  18, TIMESTAMP '2026-04-07 10:01:12'),
    (1004, 2, 20,  2,  5,  TIMESTAMP '2026-04-07 10:01:00'),
    (1004, 2, 18,  4,  5,  TIMESTAMP '2026-04-07 10:01:18'),
    (1005, 3, 150, 10, 40, TIMESTAMP '2026-04-07 10:01:00')
) AS t(sku_id, store_id, available_qty, locked_qty, in_transit_qty, update_time);
```

### 6.4 创建库存状态主键表

```sql
CREATE TABLE fluss_inventory_state_pk_s3 (
  sku_id BIGINT,
  store_id BIGINT,
  available_qty INT,
  locked_qty INT,
  in_transit_qty INT,
  update_time TIMESTAMP(3),
  PRIMARY KEY (sku_id, store_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### 6.5 写入数据

```sql
INSERT INTO fluss_inventory_state_pk_s3
SELECT *
FROM v_inventory_state;
```

### 6.6 查询结果

建议优先使用 streaming 模式观察：

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_inventory_state_pk_s3;
```

如需 batch 模式查询，建议优先做主键点查：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_inventory_state_pk_s3
WHERE sku_id = 1001 AND store_id = 1;
```

说明：

- 当前 `Fluss 0.8.0` 下，`PrimaryKey Table` 在 batch 模式更适合主键点查
- 若要观察表整体状态，优先使用 `streaming` 模式

### 6.7 预期观察点

- 同一 `(sku_id, store_id)` 多次更新后，表中体现最新态
- 库存变化顺序符合业务直觉
- `PrimaryKey Table` 在状态表场景中的语义更清晰
- 与一期现有技术栈相比，状态层表达是否更直接

---

## 七、阶段通过标准

满足以下条件即可认为第三阶段基础验证通过：

1. 场景 A 增强版跑通
2. 富化结果符合业务语义
3. 场景 B 增强版跑通
4. 库存状态最新态表现符合预期
5. 无新的重大兼容性阻塞问题

---

## 八、结果记录建议

建议记录以下内容：

1. 使用的实验库名
2. 场景 A 是否跑通
3. 场景 B 是否跑通
4. 增强数据方案是否比第二阶段更利于观察
5. 当前对 `Fluss` 在行为流特征层、库存状态层的适配性判断
6. 是否建议进入下一步更贴真实业务的小范围试点
7. 与一期当前技术栈的差异点记录

---

## 九、当前阶段结论模板

若本轮验证通过，可参考以下口径：

`Fluss` 在增强数据方案下，已完成与现有场景结合的轻量验证。  
行为流 + 商品状态补全场景与库存状态实时表场景均具备较好的可解释性与适配性。  
其中，`PrimaryKey Table` 在实时状态层候选方向上表现较好，建议继续保留为二期演进选项。

---

## 十、配套文档

- [Fluss预研规划.md](/d:/workspace/realtime-lakehouse/Fluss预研规划.md)
- [Fluss最小验证路线图.md](/d:/workspace/realtime-lakehouse/Fluss最小验证路线图.md)
- [Fluss 测试数据方案.md](/d:/workspace/realtime-lakehouse/Fluss%20测试数据方案.md)
- [Fluss初步结论.md](/d:/workspace/realtime-lakehouse/Fluss初步结论.md)
