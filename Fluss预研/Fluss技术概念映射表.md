# Fluss技术概念映射表

> 文档定位：`Fluss` 核心概念与当前实时湖仓技术栈的映射表  
> 目标：帮助团队快速理解 `Fluss` 与 `Kafka / Paimon / Flink / Doris` 的关系  
> 使用方式：作为预研、PoC 设计和技术沟通的统一术语参考

---

## 一、映射使用说明

本映射表不是说这些概念完全等价，而是帮助我们在当前项目上下文中快速建立认知对应关系。  
后续真正实施 PoC 时，应以官方语义为准，而不是生硬套用现有系统概念。

---

## 二、核心概念映射

| Fluss 概念 | 中文理解 | 在项目中的类比对象 | 说明 |
|---|---|---|---|
| Database | 数据库/命名空间 | 主题域库、实验库 | 用于组织表对象 |
| Table | 实时表 | Paimon 表 / Kafka 主题 + 表语义 | Fluss 的基础数据对象 |
| Log Table | 日志表、追加表 | Kafka 事件主题、ODS append 事件表 | 只支持 append，适合事件流 |
| PrimaryKey Table | 主键状态表 | Paimon latest 表、实时状态表 | 支持插入、更新、删除与 lookup |
| Partition | 分区 | dt / hh 分区 | 用于数据组织与裁剪 |
| Bucket | 桶/并行切分单元 | Kafka partition / shard / tablet 的综合体 | Fluss 并行与扩展核心单元 |
| Changelog | 变更日志 | CDC 变更流、Paimon changelog | 状态变化的事件化输出 |
| Partial Update | 部分列更新 | 实时宽表按字段补全 | 对状态表很有吸引力 |
| Snapshot + Incremental Read | 先快照后增量读 | latest + changelog 消费模式 | 适合状态类消费 |
| Lookup Join | 实时查维 | Flink Lookup Join | 主键表的重要能力 |
| Prefix Lookup | 前缀查找 | 前缀键扫描 | 适合部分主键查询 |
| CoordinatorServer | 控制节点 | 元数据管理与调度控制层 | 负责元数据、分配、迁移、协调 |
| TabletServer | 存储服务节点 | 数据服务节点 | 负责存储、持久化和 I/O |
| LogStore | 日志存储层 | 日志型底层存储 | 服务 Log Table |
| KvStore | 键值状态存储层 | 状态型底层存储 | 服务 PrimaryKey Table |

---

## 三、与当前实时湖仓分层的映射建议

### 3.1 ODS 层

更接近 `Fluss` 的对象：

- 行为埋点流
- 物流轨迹事件流
- 社媒抓取事件流
- 原始事件 append 事实流

建议优先关注：

- `Log Table`

### 3.2 DIM / 实时状态层

更接近 `Fluss` 的对象：

- 商品实时状态
- 店仓 SKU 状态
- 订单实时状态
- 实时特征状态表

建议优先关注：

- `PrimaryKey Table`
- `Lookup Join`
- `Partial Update`

### 3.3 DWD / DWS 层

对 `Fluss` 来说，这两层不一定是最先切入的地方。  
当前更合理的角色是：

- 作为 DWD/DWS 前的实时中间层
- 为 DWD 打宽或 DWS 聚合提供更低延迟状态/事件输入

### 3.4 ADS / 服务层

当前不建议把 `Fluss` 直接作为 ADS 服务层承载。  
本项目现阶段 ADS 仍建议由 `Doris` 负责。

---

## 四、与现有技术栈组件的关系

| 当前组件 | 当前角色 | Fluss 与它的关系 | 当前判断 |
|---|---|---|---|
| Kafka | 事件流入口 | Fluss 可对部分事件流场景形成替代或补充 | 需审慎评估，不宜直接替换 |
| Flink | 实时计算引擎 | Fluss 的核心集成对象 | 强相关，是 PoC 主入口 |
| Paimon | 湖仓主存储 | Fluss 不建议直接全量替代 | 更适合局部对比与补充 |
| Doris | 查询与服务层 | Fluss 不是其替代品 | 两者角色不同，可互补 |
| Superset | BI 消费层 | 无直接替代关系 | 基本无冲突 |

---

## 五、对一期项目的价值映射

| 一期对象 | 当前承载方式 | Fluss 候选承载方式 | 预研价值 |
|---|---|---|---|
| 用户行为事件流 | Kafka + Paimon | Log Table | 看是否降低链路复杂度/时延 |
| 社媒事件流 | Kafka + Paimon | Log Table | 看是否更适合事件实时处理 |
| 库存状态 | Paimon latest / DWD | PrimaryKey Table | 看是否更适合作为实时状态层 |
| 爆款实时特征 | DWD / DWS 中间聚合 | Log/PrimaryKey Table | 看是否适合做前置特征层 |
| 实时查维/状态 enrichment | Paimon / Flink | PrimaryKey Table + Lookup | 看 lookup 能力和延迟表现 |

---

## 六、当前推荐的理解方式

为了避免误解，团队内部建议统一采用以下说法：

- 不说“Fluss 替代 Kafka”
- 不说“Fluss 替代 Paimon”
- 建议说：
  - `Fluss` 是可评估的实时数据层候选
  - `Fluss` 可能在部分事件流和主键状态流场景中提供更自然的承载方式
  - `Fluss` 是否值得引入，取决于 PoC 是否能证明它在某段链路上优于现有方案

---

## 七、当前结论

当前最值得进入 PoC 的映射关系是：

1. `Log Table` -> 行为/物流/社媒事件流
2. `PrimaryKey Table` -> 实时库存状态层 / 实时特征状态层
3. `Lookup Join` -> DWD 实时 enrichment / 实时特征补全

---

## 附录：参考资料

- [Fluss预研规划.md](/d:/workspace/realtime-lakehouse/Fluss预研规划.md)
- [Fluss预研笔记.md](/d:/workspace/realtime-lakehouse/Fluss预研笔记.md)
- Fluss Docs: https://fluss.apache.org/docs/
