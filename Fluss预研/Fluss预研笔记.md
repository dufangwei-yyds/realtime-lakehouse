# Fluss预研笔记

> 文档定位：`Apache Fluss` 第一阶段预研笔记  
> 目标：梳理核心概念、组件部署、表模型与 `Flink` 集成方式，为后续最小链路 PoC 做准备  
> 适用阶段：预研第一阶段  
> 参考来源：项目现有方案文档 + `Apache Fluss` 官方文档

---

## 一、预研目标回顾

本阶段不直接做生产替换，也不直接做复杂业务迁移，而是先回答 4 个问题：

1. `Fluss` 的核心概念是什么
2. `Fluss` 集群由哪些组件构成
3. `Fluss` 的表模型与我们当前实时湖仓的哪些对象最接近
4. `Fluss` 如何与 `Flink` 做最小闭环集成

---

## 二、Fluss 的核心定位

根据官方文档，`Fluss` 是面向“实时分析与 AI”的 `streaming storage`，强调以下几个关键词：

- 流式存储
- 低延迟读写
- 面向实时分析
- 面向 AI / 特征类场景
- 与 `Apache Flink` 紧密集成

放到我们当前项目语境里，可以把它先理解成：

- 不是传统消息队列的简单替代品
- 也不是离线湖仓表存储的直接替代品
- 更像“介于流与表之间的实时数据层”

它最值得关注的，不是“能不能存数据”，而是：

- 能不能更自然地承载实时事件流
- 能不能更自然地承载主键状态流
- 能不能为实时 Join、实时特征和低延迟读取提供更直接的数据层能力

---

## 三、Fluss 核心概念梳理

### 3.1 Database

`Database` 是表的逻辑集合，作用上类似我们熟悉的数据库命名空间。

在预研阶段可以把它理解成：

- 一个场景域
- 一个实验空间
- 一组主题表的管理边界

### 3.2 Table

`Table` 是 Fluss 用户数据的基本存储对象，组织形式仍然是行列式逻辑表，但其能力更偏实时流式读写。

官方文档中，表按是否存在主键分成两类：

- `Log Table`
- `PrimaryKey Table`

### 3.3 Partition

定义了分区列后，表会成为分区表。  
这和我们当前在 `Paimon` 里做日期分区、小时分区的思路类似，但在 `Fluss` 中，它更多服务于数据组织与读取边界。

### 3.4 Bucket

`Bucket` 是 Fluss 并行与扩展的基础单元。  
这一点非常重要，后面做 PoC 时需要特别关注：

- 表会按 bucket 切分
- `bucket.num` 是建表的关键参数
- 对 `PrimaryKey Table` 而言，bucket 的分配与主键/桶键有关

在我们的项目里，它可以先类比理解为：

- 类似 Kafka 的分区并行单元
- 也类似分布式表存储中的 shard / tablet 切分单元

但它不是简单等同关系，后续 PoC 要重点验证它对写入、读取、lookup 的影响。

---

## 四、Fluss 两类核心表模型

### 4.1 Log Table

`Log Table` 是 append-only 表，只支持追加写，不支持 Update/Delete。

官方定义要点：

- 不定义 `PRIMARY KEY`
- 常用于高吞吐日志类数据
- 写入顺序敏感
- 更接近典型事件流、日志流、行为流场景

从我们项目视角看，`Log Table` 最接近以下数据：

- 用户行为埋点事件流
- 社媒抓取事件流
- 物流轨迹事件流
- 实时爆款原始事件流

也就是说，它更像我们现在：

- `Kafka` 中的事件主题
- `ODS` 中 append 型事件事实

### 4.2 PrimaryKey Table

`PrimaryKey Table` 支持：

- `INSERT`
- `UPDATE`
- `DELETE`

并保证主键唯一。若写入相同主键的多条记录，最终保留最新值。

官方定义要点：

- 通过 `PRIMARY KEY (...) NOT ENFORCED` 创建
- 分区主键表时，主键必须包含分区键
- 支持 changelog
- 支持 lookup / prefix lookup
- 支持 partial update

这一类表对我们项目非常关键，因为它天然贴合以下对象：

- 实时库存状态
- 实时订单状态
- 商品实时状态
- 店仓 SKU 状态宽表
- 实时特征状态表

如果后续 PoC 验证结果良好，`PrimaryKey Table` 很可能是 `Fluss` 最值得关注的能力点。

### 4.3 Partial Update

官方文档提到 `PrimaryKey Table` 支持部分列更新。  
这意味着：

- 不一定每次都写整行
- 可按主键增量补全或更新部分字段

这对以下场景很有吸引力：

- 先写库存主状态，再补充调拨状态
- 先写商品基础信息，再补充实时特征字段
- 做特征侧宽表时分步骤更新

### 4.4 Changelog

`PrimaryKey Table` 会生成变更日志，能输出：

- `+I`
- `-U`
- `+U`
- `-D`

这意味着它不仅是状态存储，也保留“状态变化”这层语义。  
这和我们当前实时湖仓里既想保留“最新态”，又想保留“变化轨迹”的需求很贴近。

---

## 五、组件部署与架构理解

根据官方架构文档，`Fluss` 集群主要包含两个核心进程：

- `CoordinatorServer`
- `TabletServer`

### 5.1 CoordinatorServer

可以理解为控制面角色，负责：

- 元数据维护
- tablet/bucket 分配
- 节点管理
- 权限处理
- 扩缩容时的数据重平衡与迁移协调

在我们的项目里，可以把它类比理解为：

- 集群大脑
- 元数据与资源编排控制层

### 5.2 TabletServer

`TabletServer` 负责：

- 数据存储
- 持久化
- 面向用户提供 I/O 服务

官方文档还提到其包含：

- `LogStore`
- `KvStore`

这点很关键，因为它正好映射到 Fluss 两类核心数据形态：

- 日志式数据
- 主键型状态数据

### 5.3 当前对部署的初步判断

对于我们的预研阶段，不建议一开始就追求复杂部署，而应优先关注：

- 最小可运行部署方式
- 与 `Flink SQL Client` 的联通方式
- 如何创建 catalog
- 如何创建 `Log Table / PrimaryKey Table`
- 如何做最小写入、最小读取和最小 lookup

---

## 六、与 Flink 集成的关键认识

### 6.1 官方集成定位

根据官方文档，`Fluss` 与 `Flink` 的集成是其核心路径之一。

目前官方文档中能明确看到：

- 支持 `Flink SQL`
- 支持 `Table API`
- 在较新版本文档中，已明确支持 `DataStream API`
- 支持 streaming / batch read
- 支持 lookup join

### 6.2 版本观察

这一点要特别记录：

- `0.6` 文档更强调 `Table API`
- `0.8` 文档已经明确写出支持 `Table API` 与 `DataStream API`

所以后续 PoC 时，必须优先确认：

- 我们预研选用的 `Fluss` 版本
- 对应支持的 `Flink` 版本
- `Flink 1.20` 是否采用官方推荐 connector 版本

### 6.3 当前与我们项目的适配度判断

我们一期主链路是：

- `Flink 1.20.x`
- `Flink SQL`
- 少量 `DataStream API`

而官方资料显示 `Fluss` 与 `Flink 1.20` 兼容是成立的。  
这意味着它在“最小 PoC”层面是有现实可行性的。

---

## 七、Fluss 与 Flink 的最小样例思路

根据官方 quick start 与 Flink engine 文档，最小路径大致如下：

1. 在 `Flink SQL Client` 中创建 `Fluss Catalog`
2. 创建数据库
3. 创建 `Log Table`
4. 创建 `PrimaryKey Table`
5. 从临时 source 写入 `Fluss`
6. 对 `Fluss` 表做流式读取
7. 对 `PrimaryKey Table` 做 lookup join

其中最关键的第一步通常是：

```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
```

这一步给我们的直接启发是：

- `Fluss` 与 `Flink` 的接入入口是 catalog
- 使用体验更接近“表格式实时存储”
- 后续如果我们做 PoC，完全可以从 SQL 路线先入手

---

## 八、对我们项目最有价值的能力初判

结合当前项目，我认为 `Fluss` 最值得继续跟进的能力有 4 个。

### 8.1 PrimaryKey Table

这是当前最有潜力的一块，因为它非常贴合：

- 实时库存状态
- 实时订单状态
- 实时宽表
- 实时特征表

### 8.2 Lookup Join

官方明确支持基于 `PrimaryKey Table` 的 lookup join。  
这和我们现在 `DIM -> DWD`、实时 enrichment 的常见诉求天然相关。

### 8.3 Streaming Read

官方默认支持“先 snapshot，再增量”的 streaming read。  
这和实时状态表的消费方式很像，也适合我们去评估它做状态层时的消费体验。

### 8.4 Log Table

如果要观察它对行为流、物流流、社媒流的承载能力，`Log Table` 是最直接入口。

---

## 九、当前不确定点

虽然方向清晰，但目前还有几个点需要在第二阶段 PoC 中继续验证：

- 真实部署复杂度
- 对我们现有监控运维体系的接入难度
- 与 `Paimon` 相比的边界差异
- 在小规模实验下是否能体现足够价值
- 是否值得进入二期小范围试点

---

## 十、第一阶段结论

第一阶段的结论可以先定为：

1. `Fluss` 值得继续做最小 PoC
2. 最值得验证的是 `PrimaryKey Table + Lookup + Streaming Read`
3. 预研入口建议优先选 `Flink SQL`
4. 预研场景建议优先选“行为流特征层”或“库存状态层”
5. 当前阶段仍坚持“旁路预研，不动主生产链路”

---

## 附录：本轮主要参考资料

- 项目资料：
  - [一期项目结项总结.md](/d:/workspace/realtime-lakehouse/一期项目结项总结.md)
  - [技术选型报告.md](/d:/workspace/realtime-lakehouse/技术选型报告.md)
  - [遗留问题与后续迭代计划.md](/d:/workspace/realtime-lakehouse/遗留问题与后续迭代计划.md)
  - [实时湖仓构建解决方案.md](/d:/workspace/realtime-lakehouse/实时湖仓构建解决方案.md)
- 官方资料：
  - Fluss Docs: https://fluss.apache.org/docs/
  - Fluss Architecture: https://fluss.apache.org/docs/concepts/architecture/
  - Fluss Table Overview: https://fluss.apache.org/docs/0.5/table-design/overview/
  - Fluss Log Table: https://fluss.apache.org/docs/0.6/table-design/table-types/log-table/
  - Fluss PrimaryKey Table: https://fluss.apache.org/docs/0.6/table-design/table-types/pk-table/
  - Flink Engine Getting Started: https://fluss.apache.org/docs/engine-flink/getting-started/
  - Flink Reads: https://fluss.apache.org/docs/engine-flink/reads/
  - Flink Lookup Joins: https://fluss.apache.org/docs/engine-flink/lookups/
  - Quick Start with Flink: https://fluss.apache.org/docs/quickstart/flink/
