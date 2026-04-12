# Doris AI功能实验规划

> 文档定位：服装行业实时湖仓项目二阶段 `Doris AI` 功能实验规划文档  
> 适用阶段：一期项目结项后，完成 `Fluss` 预研收口，进入 `Doris AI` 功能实验规划阶段  
> 当前定位：不改动一期主生产链路，优先围绕 `AI Functions / Text Search / Vector Search` 做轻量、可解释、可展示的实验验证

---

## 一、规划背景

一期项目已经完成：

- `ODS -> DIM -> DWD -> DWS -> ADS` 全链路建设与验证
- Superset BI 看板与数据服务接口验证
- 治理运维、巡检、发布回滚与验收材料补齐

在此基础上，项目进入“二阶段预研与轻量扩展”阶段。  
在技术选型与结项规划中，`Doris 4.x` 被明确作为：

- 一期实时主题服务与高并发查询平台
- 后续 `AI Function / Text Search / Vector Search` 的实验载体

因此本阶段的重点，不是重新建设数仓主链路，而是：

- 利用一期已沉淀的数据资产
- 围绕 `Doris AI` 原生能力做轻量实验
- 识别其在服装行业实时湖仓中的现实结合点

---

## 二、规划目标

本轮 `Doris AI` 功能实验的目标分为 4 类。

### 2.1 能力边界目标

- 明确 `Doris 4.x` 当前官方支持的 AI 能力边界
- 明确哪些能力更适合做本项目实验
- 明确哪些能力当前不适合直接纳入生产承诺

### 2.2 场景结合目标

- 识别与一期项目最相关的 AI 应用方向
- 明确哪些数据对象最适合进入实验
- 形成与库存、爆款、社媒、商品知识、客服文本相关的实验切入点

### 2.3 实施准备目标

- 盘点现有实践资产
- 形成实验前提、数据输入、预期输出与验证方式
- 避免实验阶段重新摸索基础能力

### 2.4 演进决策目标

- 给出“建议先做哪些实验、不建议先做哪些实验”的优先级
- 为后续输出 `Doris AI` 实验操作文档、实验记录和结论报告做准备

---

## 三、官方能力边界梳理

根据 `Apache Doris 4.x` 官方文档，当前与本项目最相关的 AI 能力主要有 4 类：

1. `AI Functions`
2. `Text Search`
3. `Vector Search`
4. `Doris MCP Server`

本轮规划优先聚焦前三类，`MCP Server` 暂作为扩展项观察。

### 3.1 AI Overview

官方 `AI Overview` 明确将 Doris 定位为：

- 支持全文检索、向量检索、AI 函数和 MCP 智能交互的统一 AI 数据栈
- 适用于 hybrid search、semantic search、RAG、agent facing analytics 等场景

对本项目的启发是：

- `Doris AI` 更适合作为“消费层 / 服务层 / 智能分析层”能力增强
- 而不是替代实时湖仓基础分层建设

### 3.2 AI Functions

官方 `AI Functions` 当前支持的内置能力包括：

- `AI_CLASSIFY`
- `AI_EXTRACT`
- `AI_FILTER`
- `AI_FIXGRAMMAR`
- `AI_GENERATE`
- `AI_MASK`
- `AI_SENTIMENT`
- `AI_SIMILARITY`
- `AI_SUMMARIZE`
- `AI_TRANSLATE`
- `AI_AGG`

官方说明要点：

- 通过 `RESOURCE type = 'ai'` 接入外部 AI 提供方
- 能在 SQL 中直接调用 AI 函数
- 适合做文本分类、提取、情感分析、摘要、翻译、聚合等任务

### 3.3 Text Search

官方 `Text Search` 当前强调：

- 适合“精确、可解释、关键词命中型”检索
- 支持倒排索引、`MATCH_ANY / MATCH_ALL / MATCH_PHRASE`
- 4.0+ 增强了 `score()`、`SEARCH()` 与 BM25 排序

对本项目最直接的价值在于：

- 商品知识检索
- 社媒内容检索
- 客服/工单文本检索
- 运营文本检索

### 3.4 Vector Search

官方 `Vector Search` 当前强调：

- 向量检索适合语义相似检索
- 可与过滤条件结合
- 可与 Text Search 形成 hybrid search

官方实践要点包括：

- 向量列一般为 `ARRAY<FLOAT>`
- 向量索引当前更适合 ANN TopN 场景
- 需要注意过滤条件、索引与内存配置的平衡

对本项目的直接价值在于：

- 相似商品召回
- 相似社媒内容聚类
- 相似投诉/工单识别
- 商品语义推荐与知识增强

---

## 四、本项目现有 AI 实践资产盘点

当前项目内已经具备较好的 AI 实验基础，最重要的现有资产是：

### 4.1 已有实践文档

- [可用状态/实时湖仓技术栈AI增强实践.md](/d:/workspace/realtime-lakehouse/可用状态/实时湖仓技术栈AI增强实践.md)

这份文档已经覆盖：

- Doris 全文检索
- Doris 向量检索
- Doris MCP Server
- Doris AI Functions
- Flink AI 功能

这意味着我们当前不需要从 0 开始摸索，而是可以基于已有实践资产做“项目化收口”。

### 4.2 一期已沉淀的数据资产

当前可直接为 `Doris AI` 实验提供输入的数据对象包括：

- 商品维度宽表
- 订单与库存主题数据
- 社媒舆情明细与 ADS 数据
- 行为流明细与爆款相关主题
- 客服、评价、运营文本类实验样例数据

### 4.3 一期已完成的消费侧载体

当前项目中，`Doris AI` 后续实验结果可天然衔接到：

- Superset 看板
- 数据服务接口
- ADS 主题表

这使得实验结果不仅能“跑通”，也更容易做可视化展示与结果验证。

---

## 五、与一期项目的结合点识别

本轮我建议优先从 4 类结合点切入。

### 5.1 商品知识增强

可结合能力：

- `Text Search`
- `Vector Search`
- `AI_EXTRACT`
- `AI_TRANSLATE`

可结合数据：

- 商品名称
- 商品卖点描述
- 商品标签
- 属性信息

实验价值：

- 商品搜索增强
- 相似商品召回
- 标签提取与标准化

### 5.2 社媒舆情增强

可结合能力：

- `AI_SENTIMENT`
- `AI_SUMMARIZE`
- `AI_CLASSIFY`
- `Text Search`
- `Vector Search`

可结合数据：

- 小红书/社媒抓取内容
- 评论/互动文本
- 社媒 ADS 主题结果

实验价值：

- 舆情情绪判断
- 热点内容摘要
- 话题聚类与相似内容检索
- 爆款趋势识别增强

### 5.3 客服/投诉文本增强

可结合能力：

- `AI_CLASSIFY`
- `AI_EXTRACT`
- `AI_SUMMARIZE`
- `AI_MASK`
- `Text Search`
- `Vector Search`

可结合数据：

- 客服工单
- 投诉文本
- FAQ / 知识片段

实验价值：

- 工单分类
- 问题摘要
- 敏感信息脱敏
- 相似投诉聚类

### 5.4 运营与分析助手增强

可结合能力：

- `AI_GENERATE`
- `AI_AGG`
- `AI_FILTER`
- `Text Search`

可结合对象：

- 看板指标结果
- 社媒舆情结果
- 热销/滞销榜单

实验价值：

- 运营摘要生成
- 多条文本聚合分析
- 辅助分析说明文生成

---

## 六、实验优先级建议

我建议按“低风险、易验证、强展示、贴项目”排序。

### 6.1 第一优先级

优先做：

1. 社媒文本 `AI_SENTIMENT`
2. 社媒文本 `AI_SUMMARIZE`
3. 商品文本 `Text Search`

原因：

- 数据现成
- 结果好解释
- 容易和一期爆款、社媒主题结合
- 容易做展示

### 6.2 第二优先级

优先做：

1. 商品向量检索
2. 相似社媒内容向量检索
3. `AI_CLASSIFY / AI_EXTRACT`

原因：

- 价值高
- 但对 embedding、向量列和数据准备要求更高

### 6.3 第三优先级

后续观察：

1. `AI_GENERATE`
2. `AI_AGG`
3. `MCP Server`

原因：

- 更偏增强型能力
- 更适合在前两级实验跑通后再展开

---

## 七、本轮建议纳入的实验项

本轮我建议正式纳入规划的实验项有 5 个。

### 7.1 实验项 A：社媒情感分析

能力：

- `AI_SENTIMENT`

目标：

- 对社媒文本判断正负向或中性情绪
- 观察与爆款/负面舆情监控的结合潜力

### 7.2 实验项 B：社媒内容摘要

能力：

- `AI_SUMMARIZE`

目标：

- 对热点社媒内容形成摘要
- 为运营看板或专题分析提供摘要能力

### 7.3 实验项 C：商品文本全文检索

能力：

- `Text Search`

目标：

- 对商品名称、卖点、标签做全文检索
- 验证关键词检索在商品知识增强中的价值

### 7.4 实验项 D：商品向量检索

能力：

- `Vector Search`

目标：

- 验证相似商品召回
- 观察是否适合做“相似商品推荐/替代款推荐”

### 7.5 实验项 E：工单/投诉分类与脱敏

能力：

- `AI_CLASSIFY`
- `AI_MASK`

目标：

- 验证文本分类与敏感信息脱敏能力
- 为后续客服质检、服务主题分析预留方向

---

## 八、本轮暂不优先纳入的内容

当前不建议优先纳入：

- 直接面向生产的 Agent 闭环
- 大规模 RAG 系统建设
- 与实时主链路深耦合的改造
- 一上来就做复杂多模态方案

原因：

- 当前阶段重点是“轻量实验 + 项目结合”
- 不宜再次开启过重的新主线

---

## 九、建议输出物

本轮 `Doris AI` 功能实验建议形成以下输出物：

1. `Doris AI功能实验规划.md`
2. `Doris AI实验操作验证文档.md`
3. `Doris AI测试数据方案.md`
4. `Doris AI实验记录.md`
5. `Doris AI实验结论.md`

---

## 十、当前阶段结论

当前判断如下：

- `Doris AI` 的能力边界已经足够清晰
- 项目内已经具备可直接复用的 AI 实践资产
- 与一期项目结合点明确，尤其适合从：
  - 社媒文本增强
  - 商品知识增强
  - 客服文本增强
  三条线切入
- 下一步建议正式进入第一轮轻量实验设计

---

## 附录：主要参考资料

### A. 项目内资料

- [技术选型报告.md](/d:/workspace/realtime-lakehouse/技术选型报告.md)
- [遗留问题与后续迭代计划.md](/d:/workspace/realtime-lakehouse/遗留问题与后续迭代计划.md)
- [实时湖仓构建解决方案.md](/d:/workspace/realtime-lakehouse/实时湖仓构建解决方案.md)
- [可用状态/实时湖仓技术栈AI增强实践.md](/d:/workspace/realtime-lakehouse/可用状态/实时湖仓技术栈AI增强实践.md)

### B. 官方资料

- AI Overview: https://doris.apache.org/docs/4.x/ai/ai-overview/
- AI Functions Overview: https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/overview/
- Text Search Overview: https://doris.apache.org/docs/4.x/ai/text-search/overview
- Vector Search Overview: https://doris.apache.org/docs/4.x/ai/vector-search/overview/
