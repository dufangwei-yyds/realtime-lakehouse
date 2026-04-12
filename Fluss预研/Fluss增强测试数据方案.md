# Fluss增强测试数据方案

> 文档定位：`Apache Fluss` 第三阶段“与现有场景结合验证”增强测试数据方案  
> 适用阶段：完成第二阶段最小链路 PoC 后，进入场景化结合验证阶段  
> 目标：使用更贴业务语义、可控更新顺序、便于解释和便于对比的测试数据，支撑 `Fluss` 与当前一期技术栈实现的对比验证

---

## 一、方案目标

本方案服务于第三阶段两类核心动作：

1. 支撑 [Fluss场景化结合验证方案.md](/d:/workspace/realtime-lakehouse/Fluss场景化结合验证方案.md) 的实际执行
2. 为后续输出“场景对比报告”提供统一、可复现、可解释的测试数据基础

本轮增强测试数据不再只追求“能跑通”，而更强调：

- 业务语义清晰
- 主键更新顺序可控
- 结果易于解释
- 方便与一期现有技术栈实现进行对照

---

## 二、设计原则

### 2.1 业务语义优先

字段值尽量采用服装行业真实语义，而不是纯随机字符串。

例如：

- `event_type` 使用 `view / click / fav / cart / order`
- `brand_name` 使用 `TBE / LILY / JNBY`
- `product_type` 使用 `连衣裙 / 连帽夹克 / 裤装 / 上衣 / T恤`

### 2.2 有限主键空间

通过较小的主键空间，主动制造：

- 重复主键写入
- 状态覆盖
- lookup 命中

### 2.3 可控更新顺序

对库存状态类场景，不再依赖完全随机数据，而是显式构造：

- 同主键多次更新
- 由“较早状态 -> 较晚状态”的时间推进

这样更便于观察 `PrimaryKey Table` 的最新态语义。

### 2.4 便于对比

增强测试数据应同时适用于：

- `Fluss` 场景化验证
- 一期当前技术栈的对照验证

---

## 三、覆盖场景

### 3.1 场景 A：行为流 + 商品状态补全

目标：

- 验证 `Log Table + PrimaryKey Table + Lookup Join`
- 验证行为事件富化后的解释性
- 为与一期当前 `Kafka/Paimon/Flink` 方案做对照准备数据

### 3.2 场景 B：库存状态实时表

目标：

- 验证 `PrimaryKey Table` 在库存状态层的表现
- 验证主键覆盖、最新态、抽样读取
- 为与一期当前库存状态实现做对照准备数据

---

## 四、场景 A 增强数据设计

### 4.1 行为事件数据

建议字段如下：

| 字段 | 中文名 | 说明 |
|---|---|---|
| `event_id` | 事件 ID | 行为事件唯一标识 |
| `user_id` | 用户 ID | 用户标识 |
| `sku_id` | 商品 SKU ID | 与商品状态表关联键 |
| `event_type` | 行为类型 | 采用明确业务动作枚举 |
| `event_time` | 事件时间 | 行为发生时间 |

建议取值：

| `event_type` | 说明 |
|---|---|
| `view` | 浏览 |
| `click` | 点击 |
| `fav` | 收藏 |
| `cart` | 加购 |
| `order` | 下单 |

建议样例：

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

### 4.2 商品状态数据

建议字段如下：

| 字段 | 中文名 | 说明 |
|---|---|---|
| `sku_id` | 商品 SKU ID | 主键 |
| `sku_name` | 商品名称 | 商品描述名称 |
| `brand_name` | 品牌名 | 品牌维度 |
| `product_type` | 商品类型 | 服装品类 |
| `available_flag` | 可售标识 | 0/1 |
| `status_time` | 状态时间 | 商品状态更新时间 |

建议样例：

```sql
CREATE TEMPORARY VIEW v_sku_status AS
SELECT *
FROM (
  VALUES
    (1001, '春季针织连衣裙', 'TBE',  '连衣裙',   1, TIMESTAMP '2026-04-07 09:59:50'),
    (1002, '轻薄防晒夹克',   'TBE',  '连帽夹克', 1, TIMESTAMP '2026-04-07 09:59:55'),
    (1003, '高腰直筒牛仔裤', 'LILY', '裤装',     1, TIMESTAMP '2026-04-07 10:00:00'),
    (1004, '法式碎花上衣',   'LILY', '上衣',     0, TIMESTAMP '2026-04-07 10:00:03'),
    (1005, '宽松短袖T恤',    'JNBY', 'T恤',      1, TIMESTAMP '2026-04-07 10:00:05')
) AS t(sku_id, sku_name, brand_name, product_type, available_flag, status_time);
```

### 4.3 预期观察点

- 行为类型是否更贴近真实业务
- 商品状态富化后是否便于人工检查
- 不同 `sku_id` 是否能稳定命中对应商品状态
- 不可售商品 `available_flag = 0` 是否能准确体现

---

## 五、场景 B 增强数据设计

### 5.1 库存状态数据

建议字段如下：

| 字段 | 中文名 | 说明 |
|---|---|---|
| `sku_id` | 商品 SKU ID | 主键组成部分 |
| `store_id` | 门店 ID | 主键组成部分 |
| `available_qty` | 可售库存 | 当前可售量 |
| `locked_qty` | 锁定库存 | 锁定量 |
| `in_transit_qty` | 在途库存 | 在途量 |
| `update_time` | 更新时间 | 状态变更时间 |

建议样例：

```sql
CREATE TEMPORARY VIEW v_inventory_state AS
SELECT *
FROM (
  VALUES
    (1001, 1, 120, 10, 30, TIMESTAMP '2026-04-07 10:01:00'),
    (1001, 1, 118, 12, 28, TIMESTAMP '2026-04-07 10:01:10'),
    (1001, 1, 115, 15, 25, TIMESTAMP '2026-04-07 10:01:20'),
    (1002, 1,  80,  5, 12, TIMESTAMP '2026-04-07 10:01:00'),
    (1002, 1,  76,  8, 10, TIMESTAMP '2026-04-07 10:01:15'),
    (1003, 2,  66,  3, 20, TIMESTAMP '2026-04-07 10:01:00'),
    (1003, 2,  60,  6, 18, TIMESTAMP '2026-04-07 10:01:12'),
    (1004, 2,  20,  2,  5, TIMESTAMP '2026-04-07 10:01:00'),
    (1004, 2,  18,  4,  5, TIMESTAMP '2026-04-07 10:01:18'),
    (1005, 3, 150, 10, 40, TIMESTAMP '2026-04-07 10:01:00')
) AS t(sku_id, store_id, available_qty, locked_qty, in_transit_qty, update_time);
```

### 5.2 预期观察点

- 同一 `(sku_id, store_id)` 多次写入后是否保留最新态
- 状态变化是否与时间推进逻辑一致
- 数据是否足够支撑人工核对

---

## 六、与一期现有技术栈对比时的使用建议

这套增强测试数据方案建议作为统一测试底稿，同时用于：

1. `Fluss` 第三阶段验证
2. 一期当前技术栈实现的对照验证

这样后续输出“场景对比报告”时，就可以从以下维度做更公平的对照：

- 建模复杂度
- SQL 表达复杂度
- 结果可解释性
- 状态语义清晰度
- lookup / 富化实现体验

---

## 七、当前结论

本轮增强测试数据方案已经满足第三阶段的核心要求：

- 更贴业务语义
- 更便于人工核对
- 更利于场景对比
- 更适合为“是否纳入二期试点”提供事实基础

---

## 附录：配套文档

- [Fluss场景化结合验证方案.md](/d:/workspace/realtime-lakehouse/Fluss场景化结合验证方案.md)
- [Fluss PoC 操作验证文档.md](/d:/workspace/realtime-lakehouse/Fluss%20PoC%20操作验证文档.md)
- [Fluss初步结论.md](/d:/workspace/realtime-lakehouse/Fluss初步结论.md)
