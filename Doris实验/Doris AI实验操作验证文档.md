# Doris AI实验操作验证文档

> 文档定位：`Doris 4.x` AI 功能实验 OOTB 操作手册  
> 适用阶段：`Doris AI` 功能实验第一轮  
> 当前范围：优先覆盖社媒舆情增强、商品知识增强，并预留客服文本增强与向量检索扩展入口

---

## 一、实验目标

本轮手册围绕 5 个实验项组织，但优先执行前 3 个：

1. 社媒情感分析
2. 社媒内容摘要
3. 商品文本全文检索
4. 商品向量检索
5. 工单/投诉分类与脱敏

建议优先顺序：

1. 先做 `AI_SENTIMENT`
2. 再做 `AI_SUMMARIZE`
3. 再做 `Text Search`
4. 最后视时间补 `AI_CLASSIFY / AI_MASK / Vector Search`

---

## 二、前置说明

### 2.1 适用版本

- `Doris 4.x`

### 2.2 数据来源

本手册使用：

- [Doris AI测试数据方案.md](/d:/workspace/realtime-lakehouse/Doris%20AI测试数据方案.md)
- [可用状态/实时湖仓技术栈AI增强实践.md](/d:/workspace/realtime-lakehouse/可用状态/实时湖仓技术栈AI增强实践.md)

### 2.3 当前建议的实验数据库

建议新建独立实验库：

- `ai_exp`

避免与一期已完成的主题库和 ADS 库混用。

---

## 三、公共准备

### 3.1 创建实验库

```sql
CREATE DATABASE IF NOT EXISTS ai_exp;
USE ai_exp;
```

### 3.2 创建 AI Resource

根据 Doris 官方 `AI Functions Overview`，在使用 AI Function 前需要创建 `RESOURCE type = 'ai'`。

示例：

```sql
CREATE RESOURCE `ai_deepseek`
PROPERTIES (
  'type' = 'ai',
  'ai.provider_type' = 'deepseek',
  'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
  'ai.model_name' = 'deepseek-chat',
  'ai.api_key' = 'sk-1caee1f3915647ed8eab4f708b5ce24a',
  'ai.temperature' = '0.1',
  'ai.max_retries' = '3'
);
```

设置默认资源：

```sql
SET default_ai_resource = 'ai_deepseek';
```

说明：

- 具体 provider、endpoint、model_name 以你当前实验环境为准
- 若你已有可复用的 AI 资源定义，可直接复用

官方参考：

- [AI Functions Overview](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/overview/)

---

## 四、实验项 A：社媒情感分析

### 4.1 目标

验证 `AI_SENTIMENT` 是否适合：

- 识别正向/负向社媒内容
- 辅助爆款与负面舆情分析

### 4.2 创建实验表

```sql
CREATE TABLE IF NOT EXISTS ai_social_note_exp (
  note_id BIGINT,
  platform STRING,
  sku_id BIGINT,
  title STRING,
  content TEXT,
  author_name STRING,
  like_cnt INT,
  fav_cnt INT,
  comment_cnt INT,
  post_time DATETIME
)
DUPLICATE KEY(note_id)
DISTRIBUTED BY HASH(note_id) BUCKETS 4
PROPERTIES (
  "replication_num" = "1"
);
```

### 4.3 写入测试数据

请直接使用 [Doris AI测试数据方案.md](/d:/workspace/realtime-lakehouse/Doris%20AI测试数据方案.md) 中“社媒文本实验数据设计”的样例插入。

### 4.4 执行情感分析

```sql
SELECT
  note_id,
  sku_id,
  title,
  AI_SENTIMENT(content) AS sentiment_result
FROM ai_social_note_exp;
```

### 4.5 验证通过标准

- 明显正向文本返回偏正向结果
- 明显负向文本返回偏负向结果
- 中性文本结果可解释

### 4.6 建议记录

- 哪几条结果最符合预期
- 哪几条结果有偏差
- 是否适合用于舆情粗分类

---

## 五、实验项 B：社媒内容摘要

### 5.1 目标

验证 `AI_SUMMARIZE` 是否适合：

- 生成热点内容摘要
- 为运营看板提供摘要辅助

### 5.2 执行摘要实验

```sql
SELECT
  note_id,
  sku_id,
  title,
  AI_SUMMARIZE(content) AS summary_result
FROM ai_social_note_exp;
```

### 5.3 组合实验：标题 + 正文

```sql
SELECT
  note_id,
  AI_SUMMARIZE(CONCAT(title, '。', content)) AS summary_result
FROM ai_social_note_exp;
```

### 5.4 验证通过标准

- 摘要长度适中
- 能提炼核心情绪/卖点/问题
- 结果适合放入运营看板或专题分析说明

---

## 六、实验项 C：商品文本全文检索

### 6.1 目标

验证 `Text Search` 是否适合：

- 商品知识检索
- 商品卖点查找
- 标签关键词搜索

### 6.2 创建商品知识实验表

```sql
CREATE TABLE IF NOT EXISTS ai_product_knowledge_exp (
  sku_id BIGINT,
  spu_code STRING,
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  season STRING,
  style_tags TEXT,
  selling_points TEXT,
  detail_desc TEXT,
  INDEX idx_sku_name (sku_name) USING INVERTED,
  INDEX idx_selling_points (selling_points) USING INVERTED,
  INDEX idx_detail_desc (detail_desc) USING INVERTED
)
DUPLICATE KEY(sku_id)
DISTRIBUTED BY HASH(sku_id) BUCKETS 4
PROPERTIES (
  "replication_num" = "1"
);
```

### 6.3 写入测试数据

请直接使用 [Doris AI测试数据方案.md](/d:/workspace/realtime-lakehouse/Doris%20AI测试数据方案.md) 中“商品知识实验数据设计”的样例插入。

### 6.4 基础检索实验

按卖点搜索：

```sql
SELECT sku_id, sku_name, selling_points
FROM ai_product_knowledge_exp
WHERE selling_points MATCH_ANY '显瘦 通勤';
```

按详细描述搜索：

```sql
SELECT sku_id, sku_name, detail_desc
FROM ai_product_knowledge_exp
WHERE detail_desc MATCH_ALL '都市 通勤';
```

按短语搜索：

```sql
SELECT sku_id, sku_name, detail_desc
FROM ai_product_knowledge_exp
WHERE detail_desc MATCH_PHRASE '修饰腿型';
```

### 6.5 验证通过标准

- 检索结果与关键词/短语语义一致
- 返回商品与人工理解一致
- 能支持商品知识增强的基础搜索需求

官方参考：

- [Text Search Overview](https://doris.apache.org/docs/4.x/ai/text-search/overview)
- [Search Operators](https://doris.apache.org/docs/4.x/ai/text-search/search-operators)

---

## 七、实验项 D：商品向量检索

### 7.1 目标

验证 `Vector Search` 是否适合：

- 相似商品召回
- 商品语义推荐

### 7.2 当前建议

这一项建议优先复用 [可用状态/实时湖仓技术栈AI增强实践.md](/d:/workspace/realtime-lakehouse/可用状态/实时湖仓技术栈AI增强实践.md) 中“2. Doris 向量检索”已有实践。

原因：

- 该部分已包含：
  - 建表
  - 向量索引
  - ANN Search
  - 业务过滤组合检索
- 直接复用比在本轮重新造一套更稳

### 7.3 本轮最低要求

至少验证：

1. 商品向量表可创建
2. 能完成一次相似商品查询
3. 能完成一次“向量检索 + 业务过滤条件”查询

---

## 八、实验项 E：工单/投诉分类与脱敏

### 8.1 目标

验证：

- `AI_CLASSIFY`
- `AI_MASK`

是否适合客服工单、投诉文本类场景

### 8.2 创建工单实验表

```sql
CREATE TABLE IF NOT EXISTS ai_service_ticket_exp (
  ticket_id BIGINT,
  user_name STRING,
  phone_no STRING,
  order_no STRING,
  ticket_type STRING,
  content TEXT,
  create_time DATETIME
)
DUPLICATE KEY(ticket_id)
DISTRIBUTED BY HASH(ticket_id) BUCKETS 4
PROPERTIES (
  "replication_num" = "1"
);
```

### 8.3 写入测试数据

请直接使用 [Doris AI测试数据方案.md](/d:/workspace/realtime-lakehouse/Doris%20AI测试数据方案.md) 中“客服工单实验数据设计”的样例插入。

### 8.4 工单分类实验

```sql
SELECT
  ticket_id,
  content,
  AI_CLASSIFY(content, ['换货', '物流投诉', '退款诉求', '商品咨询']) AS classify_result
FROM ai_service_ticket_exp;
```

### 8.5 脱敏实验

```sql
SELECT
  ticket_id,
  AI_MASK(content, ['name', 'phone_num', 'order_id']) AS masked_content
FROM ai_service_ticket_exp;
```

### 8.6 验证通过标准

- 分类结果基本符合人工判断
- 脱敏结果能屏蔽敏感信息
- 结果具备后续工单治理或客服质检的实验价值

官方参考：

- [AI_CLASSIFY](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/ai-classify/)
- [AI_MASK](https://doris.apache.org/docs/dev/sql-manual/sql-functions/ai-functions/ai-mask/)

---

## 九、建议执行顺序

建议按以下顺序执行：

1. 创建实验库与 AI Resource
2. 执行社媒情感分析
3. 执行社媒内容摘要
4. 执行商品文本全文检索
5. 视时间执行工单分类与脱敏
6. 最后复用已有向量检索实践

---

## 十、结果记录建议

建议每个实验项至少记录：

1. 使用的数据表
2. 使用的 SQL
3. 抽样结果
4. 是否符合预期
5. 偏差点
6. 是否值得继续推进

这些内容后续将用于补：

- `Doris AI实验记录.md`
- `Doris AI实验结论.md`

---

## 十一、当前结论

这份手册当前已经具备直接上手的基础，且与现有实践资产和一期数据资产形成了良好衔接。  
下一步建议你直接按优先级，从：

- 社媒情感分析
- 社媒内容摘要
- 商品文本全文检索

开始第一轮实验。

---

## 附录：参考资料

- [Doris AI功能实验规划.md](/d:/workspace/realtime-lakehouse/Doris%20AI功能实验规划.md)
- [Doris AI测试数据方案.md](/d:/workspace/realtime-lakehouse/Doris%20AI测试数据方案.md)
- [可用状态/实时湖仓技术栈AI增强实践.md](/d:/workspace/realtime-lakehouse/可用状态/实时湖仓技术栈AI增强实践.md)
- [AI Functions Overview](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/overview/)
- [Text Search Overview](https://doris.apache.org/docs/4.x/ai/text-search/overview)
- [Vector Search Overview](https://doris.apache.org/docs/4.x/ai/vector-search/overview/)
