# Fluss最小验证路线图

> 文档定位：`Fluss` 第二阶段 PoC 前的最小验证路线图  
> 目标：用最小工作量验证 `Fluss` 的关键能力，避免一开始就做重型改造  
> 使用范围：PoC 规划、任务拆解、实施顺序确认

---

## 一、路线图目标

本路线图服务于第二阶段“最小链路 PoC”，目标不是做完整业务迁移，而是用最小投入验证：

1. `Fluss` 能否顺利接入当前 `Flink 1.20` 环境
2. `Log Table` 是否适合事件流场景
3. `PrimaryKey Table` 是否适合实时状态场景
4. `Lookup Join` 是否值得继续深挖

---

## 二、最小验证原则

- 只选 1 到 2 个最小场景
- 优先走 `Flink SQL`
- 优先使用最小测试数据
- 优先验证“是否值得继续”，而不是追求一上来就做性能压测
- 不改动一期主生产链路

---

## 三、建议的三步最小验证路线

### 3.1 第一步：最小环境联通

验证目标：

- `Flink SQL Client` 能创建 `Fluss Catalog`
- 能正常创建数据库
- 能正常创建 `Log Table` 与 `PrimaryKey Table`

建议动作：

1. 确认 `Fluss` 版本与 `Flink 1.20` 兼容关系
2. 准备最小 `Fluss` 运行环境
3. 在 `Flink SQL Client` 中执行：
   - `CREATE CATALOG`
   - `CREATE DATABASE`
   - `CREATE TABLE`

通过标准：

- Catalog 创建成功
- 两类表创建成功
- 无明显 connector / version mismatch 问题

### 3.2 第二步：最小读写闭环

验证目标：

- 能把一小批测试数据写入 `Log Table`
- 能把一小批主键状态数据写入 `PrimaryKey Table`
- 能从 `Flink` 端读出数据

建议动作：

1. 准备 2 张 source 表
   - 一张 append 事件源
   - 一张主键状态源
2. 执行最小 `INSERT INTO`
3. 验证 streaming read / batch read

通过标准：

- 事件流能写入并读取
- 主键流能写入并读取
- 主键冲突场景下数据结果符合预期

### 3.3 第三步：最小 lookup 场景

验证目标：

- 使用 `PrimaryKey Table` 做 lookup join
- 观察实时 enrichment 可行性

建议动作：

1. 创建一张事实流表
2. 创建一张主键维表
3. 执行 `lookup join`

通过标准：

- join 能正常执行
- join 结果正确
- 没有明显的开发复杂度异常

---

## 四、建议优先验证的两个最小场景

### 4.1 场景 A：行为流 + 商品状态补全

结构：

- 行为事件 -> `Log Table`
- 商品状态 -> `PrimaryKey Table`
- `Flink Lookup Join` -> 富化输出

为什么优先：

- 贴近我们现有爆款识别场景
- 事件流和状态流能同时验证
- 对现有主链路侵入小

### 4.2 场景 B：库存状态实时表

结构：

- 库存状态写入 `PrimaryKey Table`
- 从 `Flink` 读取 snapshot + incremental

为什么优先：

- 能直观验证主键表能力
- 贴近我们现有库存 ATP / 调拨场景
- 对 `PrimaryKey Table` 的价值判断最直接

---

## 五、推荐的最小样例结构

### 5.1 Log Table 样例方向

可选字段：

- `event_id`
- `user_id`
- `sku_id`
- `event_type`
- `event_time`

目标：

- 验证 append-only 写入
- 验证流式读取

### 5.2 PrimaryKey Table 样例方向

可选字段：

- `sku_id`
- `store_id`
- `available_qty`
- `locked_qty`
- `in_transit_qty`
- `update_time`
- `PRIMARY KEY (sku_id, store_id)`

目标：

- 验证主键更新
- 验证最新态读取
- 验证 lookup 能力

### 5.3 Lookup Join 样例方向

可选关系：

- 行为事件流 `JOIN` 商品实时状态表

输出目标：

- 生成带库存状态/商品状态的富化事件

---

## 六、建议的实施顺序

1. 先完成环境与版本确认
2. 再跑 `CREATE CATALOG / CREATE TABLE`
3. 再做 `Log Table` 最小写入读取
4. 再做 `PrimaryKey Table` 最小写入读取
5. 最后再做 `lookup join`

不建议的顺序：

- 一开始就做复杂场景
- 一开始就做多表 join
- 一开始就引入真实业务全量模型

---

## 七、第二阶段建议输出物

基于本路线图，第二阶段完成后建议形成：

1. `Fluss PoC 操作验证文档.md`
2. `Fluss 最小读写实验记录.md`
3. `Fluss Lookup Join 验证记录.md`
4. `Fluss 初步结论.md`

---

## 八、当前阶段结论

当前最合理的推进方式是：

- 以 `Flink SQL` 为主入口
- 先验证 `Log Table`
- 再验证 `PrimaryKey Table`
- 最后用 `lookup join` 来判断 `Fluss` 是否真的对我们现有实时湖仓有额外价值

如果这三步走通，再进入下一阶段的场景化 PoC，会更稳。

---

## 附录：配套文档

- [Fluss预研规划.md](/d:/workspace/realtime-lakehouse/Fluss预研规划.md)
- [Fluss预研笔记.md](/d:/workspace/realtime-lakehouse/Fluss预研笔记.md)
- [Fluss技术概念映射表.md](/d:/workspace/realtime-lakehouse/Fluss技术概念映射表.md)
