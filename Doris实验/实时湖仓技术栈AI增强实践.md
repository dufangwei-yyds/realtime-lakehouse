# 实时湖仓技术栈 AI 增强实践

> 本文档涵盖 Doris 全文检索、向量检索、MCP Server、AI Functions 以及 Flink AI 功能的完整实践指南。

---

## 目录

1. [Doris 全文检索 (Full-Text Search)](#1-doris-全文检索-full-text-search)
2. [Doris 向量检索 (Vector Search)](#2-doris-向量检索-vector-search)
3. [Doris MCP Server](#3-doris-mcp-server)
4. [Doris AI Functions](#4-doris-ai-functions)
5. [Flink AI 功能](#5-flink-ai-功能)

---

## 1. Doris 全文检索 (Full-Text Search)

### 1.1 目标

- 覆盖 Doris 4.x 文本检索四大能力：Rich Text Operators、Custom Analyzers、BM25 Relevance Scoring、SEARCH Unified Query DSL
- **场景**：电商服装商品中心 + 导购检索（标题、卖点、问答、标签）

### 1.2 能力概述

| 能力                       | 说明                                                                                 |
| -------------------------- | ------------------------------------------------------------------------------------ |
| **Rich Text Operators**    | MATCH_ANY / MATCH_ALL / MATCH_PHRASE / MATCH_PHRASE_PREFIX / MATCH_REGEXP / SEARCH() |
| **Custom Analyzers**       | 支持自定义分析管道：char_filter + tokenizer + token_filter                           |
| **BM25 Relevance Scoring** | 实现 BM25 算法进行文本相关性评分，支持 Top-N 排名                                    |
| **SEARCH Function**        | 统一查询 DSL，简洁富有表现力的语法                                                   |

### 1.3 SEARCH 语法详解

#### 基本语法

```sql
-- 单术语查询
SEARCH('column:term')

-- 任一术语匹配 (OR)
SEARCH('column:ANY(term1 term2)')

-- 所有术语匹配 (AND)
SEARCH('column:ALL(term1 term2)')

-- 精确匹配（大小写敏感）
SEARCH('column:EXACT(exact text)')
```

#### 布尔运算

```sql
-- AND 运算
SEARCH('title:apache AND category:database')

-- OR 运算
SEARCH('title:doris OR title:clickhouse')

-- NOT 运算
SEARCH('tags:ANY(olap analytics) AND NOT status:deprecated')
```

#### 多列查询

```sql
SEARCH('title:search AND (content:engine OR tags:ANY(elasticsearch lucene))')
```

#### 半结构化数据查询

```sql
SEARCH('properties.user.name:alice')
```

#### 带评分排序

```sql
SELECT id, title, score() AS relevance
FROM docs
WHERE SEARCH('title:Machine AND tags:ANY(database sql)')
ORDER BY relevance DESC
LIMIT 20;
```

---

### 1.4 实战：服装商品搜索

#### Step 1：准备库表与自定义分词器

```sql
CREATE DATABASE IF NOT EXISTS demo_doris4_ai_search;
USE demo_doris4_ai_search;

-- 自定义 tokenizer：适合中英文混合服装文案
CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS apparel_icu_tokenizer
PROPERTIES (
  "type" = "standard"
);

-- 自定义 token_filter：统一小写 + 对英文/数字连接词做切分
CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS apparel_word_splitter
PROPERTIES (
  "type" = "word_delimiter",
  "split_on_numerics" = "true",
  "split_on_case_change" = "true"
);

CREATE INVERTED INDEX ANALYZER IF NOT EXISTS apparel_cn_en_analyzer
PROPERTIES (
  "tokenizer" = "apparel_icu_tokenizer",
  "token_filter" = "asciifolding, lowercase, apparel_word_splitter"
);

-- 注意：创建 custom analyzer 后，等待约 10 秒让 FE/BE 元数据同步

DROP TABLE IF EXISTS apparel_search_docs;
CREATE TABLE apparel_search_docs (
  product_id BIGINT,
  sku_code VARCHAR(64),
  brand VARCHAR(64),
  category VARCHAR(64),
  title TEXT,
  description TEXT,
  material_care TEXT,
  customer_qa TEXT,
  tags TEXT,
  season VARCHAR(32),
  on_shelf_time DATETIME,
  INDEX idx_title(title) USING INVERTED PROPERTIES("analyzer"="apparel_cn_en_analyzer", "support_phrase"="true"),
  INDEX idx_desc(description) USING INVERTED PROPERTIES("analyzer"="apparel_cn_en_analyzer", "support_phrase"="true"),
  INDEX idx_qa(customer_qa) USING INVERTED PROPERTIES("analyzer"="apparel_cn_en_analyzer", "support_phrase"="true"),
  INDEX idx_tags(tags) USING INVERTED PROPERTIES("analyzer"="apparel_cn_en_analyzer"),
  INDEX idx_sku(sku_code) USING INVERTED PROPERTIES("built_in_analyzer"="none")
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES ("replication_num"="1");
```

#### Step 2：插入测试数据（12 条）

```sql
INSERT INTO apparel_search_docs VALUES
(1001,'JK-0001','LUMI','外套','抗皱通勤西装外套 女款','高支混纺面料，免烫抗皱，适合办公室通勤与会议场景。','建议手洗或轻柔机洗，悬挂晾干。','这款西装夏天闷吗？答：透气里布，空调房友好。','通勤 西装 抗皱 会议','all','2026-01-10 10:00:00'),
(1002,'TS-0002','LUMI','T恤','速干运动T恤 男款','轻量速干，跑步健身排汗快，版型偏修身。','可机洗，避免高温烘干。','会不会透？答：浅色建议搭配浅色内搭。','速干 运动 跑步 健身','summer','2026-01-12 10:00:00'),
(1003,'DR-0003','MIRA','连衣裙','法式碎花雪纺连衣裙','垂感雪纺，收腰显瘦，约会通勤都可穿。','建议冷水手洗，避免长时间浸泡。','肩宽女生能穿吗？答：有S到XL，建议按肩宽选码。','法式 连衣裙 雪纺 碎花','spring','2026-01-14 10:00:00'),
(1004,'CF-0004','NORTHWIND','外套','防泼水轻量冲锋衣','户外通勤两用，防泼水面料，防风立领，早晚温差友好。','轻柔机洗，不可漂白。','下雨能穿吗？答：小雨防泼水可以，暴雨建议专业雨衣。','防泼水 冲锋衣 户外 防风','autumn','2026-01-16 10:00:00'),
(1005,'CT-0005','NORTHWIND','大衣','羊毛呢中长款大衣','含羊毛材质，保暖有型，通勤正式场合皆可。','建议干洗，避免频繁水洗。','会起球吗？答：正常摩擦会有轻微起球，建议用去球器。','羊毛 呢大衣 保暖 通勤','winter','2026-01-18 10:00:00'),
(1006,'JE-0006','URB','牛仔裤','高腰牛仔阔腿裤','高腰显腿长，面料挺括，适合日常通勤。','深浅色分开洗。','裤长对小个子友好吗？答：有加短版。','牛仔 阔腿裤 高腰 通勤','all','2026-01-20 10:00:00'),
(1007,'SH-0007','URB','衬衫','凉感防晒衬衫','UPF 防晒，凉感面料，适合通勤与户外。','建议冷水机洗，自然晾干。','夏季闷热吗？答：面料轻薄，透气性较好。','防晒 凉感 衬衫 通勤','summer','2026-01-22 10:00:00'),
(1008,'IN-0008','HEATUP','内搭','德绒保暖内搭上衣','亲肤弹力，秋冬打底，锁温效果明显。','建议反面轻柔清洗。','贴身会扎吗？答：不扎，面料偏柔软。','保暖 内搭 德绒 秋冬','winter','2026-01-24 10:00:00'),
(1009,'SH-0009','LINO','衬衫','亚麻短袖衬衫','天然亚麻，透气不闷，度假与日常都适合。','洗后轻微褶皱属正常，建议蒸汽熨烫。','容易皱吗？答：亚麻材质会有自然褶皱。','亚麻 短袖 透气 度假','summer','2026-01-26 10:00:00'),
(1010,'VT-0010','NORTHWIND','马甲','户外机能多口袋马甲','多口袋收纳，轻户外摄影穿搭，叠穿友好。','建议冷水手洗。','适合通勤吗？答：偏户外机能风，通勤可搭基础内搭。','机能 马甲 户外 多口袋','autumn','2026-01-28 10:00:00'),
(1011,'HD-0011','LUMI','卫衣','直播爆款连帽卫衣','宽松落肩版型，绒感舒适，适合日常休闲。','可机洗，避免暴晒。','会掉色吗？答：深色前两次建议单独洗。','卫衣 连帽 宽松 休闲','all','2026-01-30 10:00:00'),
(1012,'LF-0012','MIRA','礼服','婚礼伴娘缎面礼服','缎面光泽，修身剪裁，适合婚礼晚宴场景。','建议干洗。','是否可改短？答：可联系门店改衣服务。','礼服 伴娘 婚礼 晚宴','all','2026-02-01 10:00:00');
```

---

#### Step 3：Rich Text Operators 示例

```sql
-- A. MATCH_ANY：任一词命中（召回优先）
SELECT product_id, title
FROM apparel_search_docs
WHERE description MATCH_ANY '通勤 抗皱';

-- B. MATCH_ALL：所有词都命中（精度优先）
SELECT product_id, title
FROM apparel_search_docs
WHERE description MATCH_ALL '通勤 抗皱';

-- C. MATCH_PHRASE：短语精确匹配
SELECT product_id, title
FROM apparel_search_docs
WHERE title MATCH_PHRASE '法式 碎花';

-- D. MATCH_PHRASE（slop）：允许词间距离
SELECT product_id, title
FROM apparel_search_docs
WHERE description MATCH_PHRASE '防泼水 防风 ~4';

-- E. MATCH_PHRASE_PREFIX：短语前缀（搜索联想/自动补全）
SELECT product_id, title
FROM apparel_search_docs
WHERE title MATCH_PHRASE_PREFIX '法式 碎';

-- F. MATCH_REGEXP：正则（基于词项）
SELECT product_id, title
FROM apparel_search_docs
WHERE tags MATCH_REGEXP '^防.*';
```

---

#### Step 4：Custom Analyzer 验证

```sql
-- 查看组件
SHOW INVERTED INDEX TOKENIZER;
SHOW INVERTED INDEX TOKEN_FILTER;
SHOW INVERTED INDEX ANALYZER;

-- 用 TOKENIZE 观察分词效果
SELECT TOKENIZE(
  'UPF50+防晒衬衫-Office版',
  '"analyzer"="apparel_cn_en_analyzer"'
) AS tokens_json;
```

> **验证点**：
>
> - `UPF50+`、`Office` 等中英文混合词可被合理切分并归一化（lowercase）
> - SKU 类字段用 `built_in_analyzer=none` 可支持整词精确检索

---

#### Step 5：BM25 Relevance Scoring

```sql
-- 查询"通勤 抗皱 西装"，按 BM25 score() 排序
SELECT
  product_id,
  title,
  score() AS relevance
FROM apparel_search_docs
WHERE description MATCH_ANY '通勤 抗皱 西装'
ORDER BY relevance DESC
LIMIT 5;
```

> **预期结果**：`抗皱通勤西装外套 女款` 排名前列
>
> - 分数越高，表示文本与查询词越相关（绝对值不重要，相对排序更重要）

---

#### Step 6：SEARCH Function 统一查询 DSL

```sql
-- A. 单字段 + 默认字段 + 默认AND
SELECT product_id, title
FROM apparel_search_docs
WHERE SEARCH('通勤 抗皱', 'description', 'and');

-- B. 多字段布尔组合
SELECT product_id, title
FROM apparel_search_docs
WHERE SEARCH('(title:ANY(通勤 西装) OR tags:ANY(通勤 会议))')
AND category <> '礼服';

-- C. 短语查询（引号）
SELECT product_id, title
FROM apparel_search_docs
WHERE SEARCH('description:"防泼水 面料"');

-- D. 通配符查询（前缀）
SELECT product_id, title
FROM apparel_search_docs
WHERE SEARCH('title:防*');

-- E. EXACT 精确查询（适合 none analyzer 字段）
SELECT product_id, sku_code, title
FROM apparel_search_docs
WHERE SEARCH('sku_code:EXACT(JK-0001)');

-- F. DSL + BM25 一起用（导购排序常用）
SELECT
  product_id,
  title,
  score() AS relevance
FROM apparel_search_docs
WHERE description MATCH_ANY '通勤 西装 抗皱 免烫'
  AND SEARCH('(title:ANY(通勤 西装) OR description:ALL(抗皱 免烫)) AND NOT tags:ANY(礼服)')
ORDER BY relevance DESC
LIMIT 10;
```

---

### 1.5 服装行业落地建议

| 场景         | 推荐方案                                                                           |
| ------------ | ---------------------------------------------------------------------------------- |
| 导购搜索     | 用 `MATCH_ANY` 做召回、`MATCH_ALL/MATCH_PHRASE` 做精排前过滤、`score()` 做最终排序 |
| 活动页联想词 | 用 `MATCH_PHRASE_PREFIX` 做"输入即搜"                                              |
| 运营规则检索 | 用 `SEARCH()` 把复杂布尔规则写在一条 DSL 里，减少多字段 SQL 拼接复杂度             |
| 数据治理     | 高频精确 ID（SKU/款号）建议单独建 `none` analyzer 倒排索引                         |

---

### 1.6 可复用验证清单

- [ ] **能查**：`MATCH_ANY / MATCH_ALL / MATCH_PHRASE / MATCH_PHRASE_PREFIX / MATCH_REGEXP`
- [ ] **能看**：`TOKENIZE()` 输出是否符合业务词切分预期
- [ ] **能排**：`score()` TopN 排序是否符合导购常识
- [ ] **能编排**：`SEARCH()` 是否覆盖你的多字段 + 布尔 + 精确匹配组合

---

## 2. Doris 向量检索 (Vector Search)

### 2.1 目标

- 覆盖 Approximate Nearest Neighbor Search、Approximate Range Search、Compound Search、ANN Search with Additional Filters、Session Variables Related to ANN Search、Vector Quantization
- **场景**：服装商品语义检索（标题/卖点向量）+ 业务过滤（类目/季节/价格/上架状态）

### 2.2 使用前说明

> **重要**：
>
> - 向量列必须是 `ARRAY<FLOAT> NOT NULL`，并且每行向量长度必须等于 `dim`
> - ANN 仅支持 `DUPLICATE KEY`
> - `metric_type=l2_distance` 时，查询必须用 `l2_distance_approximate()` 且 `ORDER BY dist ASC`
> - 如果要"向量检索 + 过滤条件"仍走索引，过滤列建议建立二级索引（如 INVERTED）

---

### 2.3 实战：服装商品向量检索

#### Step 1：建表（FLAT 量化）+ 过滤字段索引

```sql
CREATE DATABASE IF NOT EXISTS demo_doris4_vector_search;
USE demo_doris4_vector_search;

DROP TABLE IF EXISTS apparel_vector_docs;
CREATE TABLE apparel_vector_docs (
  product_id BIGINT NOT NULL,
  sku_code VARCHAR(64) NOT NULL,
  title STRING NOT NULL,
  category STRING NOT NULL,
  season STRING NOT NULL,
  brand STRING NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  is_on_sale TINYINT NOT NULL,
  style_tags STRING NOT NULL,
  -- 示例使用 8 维，便于手工测试；生产建议 256/512/768 维
  embedding ARRAY<FLOAT> NOT NULL,

  INDEX ann_embedding(embedding) USING ANN PROPERTIES(
    "index_type"="hnsw",
    "metric_type"="l2_distance",
    "dim"="8",
    "max_degree"="32",
    "ef_construction"="40",
    "quantizer"="flat"
  ),

  -- 为 ANN + 过滤组合查询准备二级索引
  INDEX idx_category(category) USING INVERTED,
  INDEX idx_season(season) USING INVERTED,
  INDEX idx_brand(brand) USING INVERTED,
  INDEX idx_style_tags(style_tags) USING INVERTED PROPERTIES("support_phrase"="true")
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES ("replication_num"="1");
```

#### Step 2：插入测试数据（12 条，含可比较向量）

```sql
INSERT INTO apparel_vector_docs VALUES
(2001,'JK-0001','抗皱通勤西装外套','外套','all','LUMI',399.00,1,'通勤 西装 抗皱 会议',[0.90,0.80,0.10,0.20,0.70,0.10,0.40,0.20]),
(2002,'TS-0002','速干运动T恤','T恤','summer','LUMI',129.00,1,'速干 运动 跑步 健身',[0.10,0.20,0.95,0.80,0.20,0.10,0.70,0.60]),
(2003,'DR-0003','法式碎花雪纺连衣裙','连衣裙','spring','MIRA',329.00,1,'法式 连衣裙 雪纺 约会',[0.85,0.60,0.20,0.10,0.40,0.30,0.20,0.50]),
(2004,'CF-0004','防泼水轻量冲锋衣','外套','autumn','NORTHWIND',459.00,1,'防泼水 冲锋衣 户外 防风',[0.20,0.30,0.80,0.90,0.30,0.20,0.60,0.70]),
(2005,'CT-0005','羊毛呢中长款大衣','大衣','winter','NORTHWIND',699.00,0,'羊毛 呢大衣 保暖 通勤',[0.75,0.70,0.15,0.20,0.65,0.20,0.30,0.25]),
(2006,'JE-0006','高腰牛仔阔腿裤','牛仔裤','all','URB',259.00,1,'牛仔 阔腿裤 高腰 通勤',[0.65,0.55,0.30,0.25,0.45,0.40,0.35,0.30]),
(2007,'SH-0007','凉感防晒衬衫','衬衫','summer','URB',219.00,1,'防晒 凉感 衬衫 通勤',[0.25,0.35,0.85,0.75,0.35,0.30,0.65,0.55]),
(2008,'IN-0008','德绒保暖内搭上衣','内搭','winter','HEATUP',169.00,1,'保暖 内搭 德绒 秋冬',[0.70,0.65,0.10,0.15,0.85,0.30,0.20,0.20]),
(2009,'SH-0009','亚麻短袖衬衫','衬衫','summer','LINO',239.00,1,'亚麻 短袖 透气 度假',[0.30,0.40,0.75,0.70,0.25,0.20,0.55,0.60]),
(2010,'VT-0010','户外机能多口袋马甲','马甲','autumn','NORTHWIND',299.00,1,'机能 马甲 户外 多口袋',[0.35,0.45,0.88,0.92,0.30,0.25,0.62,0.68]),
(2011,'HD-0011','直播爆款连帽卫衣','卫衣','all','LUMI',279.00,1,'卫衣 连帽 宽松 休闲',[0.55,0.50,0.45,0.40,0.50,0.55,0.40,0.45]),
(2012,'LF-0012','婚礼伴娘缎面礼服','礼服','all','MIRA',899.00,0,'礼服 伴娘 婚礼 晚宴',[0.95,0.70,0.05,0.10,0.35,0.20,0.15,0.45]);
```

#### Step 3：构造 PQ 训练集（12 条扩充为 1200 条）

```sql
DROP TABLE IF EXISTS apparel_vector_docs_train;
CREATE TABLE apparel_vector_docs_train (
  product_id BIGINT NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num"="1");

INSERT INTO apparel_vector_docs_train
SELECT
  product_id + (a.n * 10000) + (b.n * 1000) AS product_id,
  embedding
FROM apparel_vector_docs
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) a
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) b;
```

---

### 2.4 ANN 查询示例

#### Approximate Nearest Neighbor Search（ANN TopN）

```sql
-- 查询向量: 偏"通勤西装/抗皱"语义
SELECT
  product_id,
  title,
  l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) AS dist
FROM apparel_vector_docs
ORDER BY dist ASC
LIMIT 5;
```

#### Approximate Range Search（近似范围检索）

```sql
-- 统计与查询向量"足够近"的商品数量
SELECT COUNT(*) AS near_cnt
FROM apparel_vector_docs
WHERE l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) <= 0.35;

-- 列出范围内商品
SELECT
  product_id,
  title,
  l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) AS dist
FROM apparel_vector_docs
WHERE l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) <= 0.35
ORDER BY dist ASC;
```

#### Compound Search（ANN TopN + Range 条件）

```sql
-- 同时做 TopN 与距离阈值约束
SELECT
  product_id,
  title,
  l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) AS dist
FROM apparel_vector_docs
WHERE l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) <= 0.50
ORDER BY dist ASC
LIMIT 5;
```

#### ANN Search with Additional Filters（向量检索 + 业务过滤）

```sql
-- 例1：只看 summer + 在售 + 价格<=300 的相似商品
SELECT
  product_id,
  title,
  season,
  price,
  is_on_sale,
  l2_distance_approximate(embedding, [0.30,0.40,0.80,0.75,0.30,0.20,0.58,0.60]) AS dist
FROM apparel_vector_docs
WHERE season = 'summer'
  AND is_on_sale = 1
  AND price <= 300
ORDER BY dist ASC
LIMIT 5;

-- 例2：文本标签过滤 + ANN
SELECT
  product_id,
  title,
  style_tags,
  l2_distance_approximate(embedding, [0.25,0.35,0.85,0.75,0.35,0.30,0.65,0.55]) AS dist
FROM apparel_vector_docs
WHERE style_tags MATCH_ANY '防晒 户外'
ORDER BY dist ASC
LIMIT 5;
```

---

### 2.5 会话参数调优

```sql
-- 查看当前参数
SHOW VARIABLES LIKE 'hnsw_ef_search';
SHOW VARIABLES LIKE 'hnsw_check_relative_distance';
SHOW VARIABLES LIKE 'hnsw_bounded_queue';

-- 调大 ef_search：通常召回更好、延迟略升
SET hnsw_ef_search = 64;

-- 是否启用相对距离检查（精度优化）
SET hnsw_check_relative_distance = true;

-- 是否使用有界队列（性能优化）
SET hnsw_bounded_queue = true;

-- 参数生效后再跑一次 ANN TopN 对比
SELECT
  product_id,
  title,
  l2_distance_approximate(embedding, [0.88,0.78,0.12,0.20,0.68,0.12,0.38,0.22]) AS dist
FROM apparel_vector_docs
ORDER BY dist ASC
LIMIT 5;
```

---

### 2.6 向量量化对比

#### SQ8（INT8 标量量化）

```sql
DROP TABLE IF EXISTS apparel_vector_docs_sq8;
CREATE TABLE apparel_vector_docs_sq8 (
  product_id BIGINT NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  INDEX ann_embedding(embedding) USING ANN PROPERTIES(
    "index_type"="hnsw",
    "metric_type"="l2_distance",
    "dim"="8",
    "quantizer"="sq8"
  )
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num"="1");

INSERT INTO apparel_vector_docs_sq8
SELECT product_id, embedding FROM apparel_vector_docs;
```

#### SQ4（INT4 标量量化）

```sql
DROP TABLE IF EXISTS apparel_vector_docs_sq4;
CREATE TABLE apparel_vector_docs_sq4 (
  product_id BIGINT NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  INDEX ann_embedding(embedding) USING ANN PROPERTIES(
    "index_type"="hnsw",
    "metric_type"="l2_distance",
    "dim"="8",
    "quantizer"="sq4"
  )
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num"="1");

INSERT INTO apparel_vector_docs_sq4
SELECT product_id, embedding FROM apparel_vector_docs;
```

#### PQ（乘积量化）

```sql
DROP TABLE IF EXISTS apparel_vector_docs_pq;
CREATE TABLE apparel_vector_docs_pq (
  product_id BIGINT NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  INDEX ann_embedding(embedding) USING ANN PROPERTIES(
    "index_type"="hnsw",
    "metric_type"="l2_distance",
    "dim"="8",
    "quantizer"="pq",
    "pq_m"="4",
    "pq_nbits"="2"
  )
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES ("replication_num"="1");

-- 用扩样数据训练并写入 PQ（关键步骤）
INSERT INTO apparel_vector_docs_pq
SELECT product_id, embedding FROM apparel_vector_docs_train;

-- 校验行数
SELECT COUNT(*) AS train_cnt FROM apparel_vector_docs_train;
SELECT COUNT(*) AS pq_cnt FROM apparel_vector_docs_pq;
```

#### 三种量化效果对比（同一查询）

```sql
-- FLAT
SELECT product_id,
       l2_distance_approximate(embedding, [0.30,0.40,0.80,0.75,0.30,0.20,0.58,0.60]) AS dist
FROM apparel_vector_docs
ORDER BY dist ASC
LIMIT 5;

-- SQ8
SELECT product_id,
       l2_distance_approximate(embedding, [0.30,0.40,0.80,0.75,0.30,0.20,0.58,0.60]) AS dist
FROM apparel_vector_docs_sq8
ORDER BY dist ASC
LIMIT 5;

-- SQ4
SELECT product_id,
       l2_distance_approximate(embedding, [0.30,0.40,0.80,0.75,0.30,0.20,0.58,0.60]) AS dist
FROM apparel_vector_docs_sq4
ORDER BY dist ASC
LIMIT 5;

-- PQ
SELECT product_id,
       l2_distance_approximate(embedding, [0.30,0.40,0.80,0.75,0.30,0.20,0.58,0.60]) AS dist
FROM apparel_vector_docs_pq
ORDER BY dist ASC
LIMIT 5;
```

---

### 2.7 验证清单

- [ ] **ANN TopN**：能返回语义最近的服装商品
- [ ] **Range Search**：能按距离阈值做"相似度门槛"筛选
- [ ] **Compound Search**：能同时做阈值约束和 TopN
- [ ] **Additional Filters**：能先按业务条件过滤再做向量检索
- [ ] **Session Variables**：能通过 `hnsw_ef_search` 调整召回/延迟权衡
- [ ] **Quantization**：FLAT/SQ8/SQ4/PQ 可对比精度、速度、内存占用

### 2.8 常见坑位

> **注意**：
>
> - `WHERE` 中向量函数与 `ORDER BY` 方向不匹配会回退或结果不符合预期
> - `dim` 与数据向量长度不一致会直接报错
> - 过滤列无二级索引时，复杂混合查询可能回退暴力计算
> - 若使用 `inner_product`，需 `ORDER BY inner_product_approximate(...) DESC LIMIT N`

---

## 3. Doris MCP Server

### 3.1 目标

- 完成 Doris MCP Server 从 0 到 1 部署
- 通过服装行业测试数据，跑通"元数据查询 -> SQL 执行 -> Explain/Profile -> 数据治理分析"全链路

### 3.2 官方参考

| 资源                      | 链接                                              |
| ------------------------- | ------------------------------------------------- |
| Doris 4.x AI 总览         | https://doris.apache.org/docs/4.x/ai/ai-overview/ |
| Doris MCP Server 官方仓库 | https://github.com/apache/doris-mcp-server        |
| 建议版本                  | doris-mcp-server v0.6.x                           |

---

### 3.3 部署方式

#### 方式 A：PyPI 直接安装（推荐）

```bash
# 安装
pip install doris-mcp-server
# 或指定版本
pip install doris-mcp-server==0.6.0
```

```bash
# 启动 HTTP 模式（便于多客户端连接）
doris-mcp-server \
  --transport http \
  --host 0.0.0.0 \
  --port 3000 \
  --db-host 192.168.63.128 \
  --db-port 9030 \
  --db-user root \
  --db-password ""
```

```bash
# 健康检查
curl http://192.168.63.128:3000/health
```

#### 方式 B：源码部署

```bash
git clone https://github.com/apache/doris-mcp-server.git
cd doris-mcp-server
pip install -r requirements.txt

# 复制配置
cp .env.example .env
# 修改 .env 中 DORIS_HOST/DORIS_PORT/DORIS_USER/DORIS_PASSWORD

# 启动
python -m doris_mcp_server.main --transport http --host 0.0.0.0 --port 3000
```

---

### 3.4 客户端接入

#### HTTP 方式（服务独立运行）

```json
{
  "mcpServers": {
    "doris-http": {
      "url": "http://192.168.63.128:3000/mcp"
    }
  }
}
```

#### Stdio 方式（客户端托管进程）

```json
{
  "mcpServers": {
    "doris-stdio": {
      "command": "doris-mcp-server",
      "args": ["--transport", "stdio"],
      "env": {
        "DORIS_HOST": "192.168.63.128",
        "DORIS_PORT": "9030",
        "DORIS_USER": "root",
        "DORIS_PASSWORD": ""
      }
    }
  }
}
```

---

### 3.5 测试数据

```sql
CREATE DATABASE IF NOT EXISTS demo_doris_mcp_apparel;
USE demo_doris_mcp_apparel;

-- 维度表：商品
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
  product_id BIGINT,
  sku_code VARCHAR(64),
  product_name VARCHAR(128),
  category VARCHAR(64),
  brand VARCHAR(64),
  season VARCHAR(32),
  list_price DECIMAL(10,2),
  is_active TINYINT
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES ("replication_num"="1");

-- 事实表：订单
DROP TABLE IF EXISTS fact_order;
CREATE TABLE fact_order (
  order_id BIGINT,
  order_date DATE,
  user_id BIGINT,
  product_id BIGINT,
  qty INT,
  pay_amount DECIMAL(10,2),
  channel VARCHAR(32),
  province VARCHAR(32)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES ("replication_num"="1");

-- 事实表：评价
DROP TABLE IF EXISTS fact_review;
CREATE TABLE fact_review (
  review_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  rating INT,
  review_text TEXT,
  review_time DATETIME
)
DUPLICATE KEY(review_id)
DISTRIBUTED BY HASH(review_id) BUCKETS 8
PROPERTIES ("replication_num"="1");

-- 插入数据
INSERT INTO dim_product VALUES
(3001,'JK-0001','抗皱通勤西装外套','外套','LUMI','all',399.00,1),
(3002,'TS-0002','速干运动T恤','T恤','LUMI','summer',129.00,1),
(3003,'DR-0003','法式碎花雪纺连衣裙','连衣裙','MIRA','spring',329.00,1),
(3004,'CF-0004','防泼水轻量冲锋衣','外套','NORTHWIND','autumn',459.00,1),
(3005,'CT-0005','羊毛呢中长款大衣','大衣','NORTHWIND','winter',699.00,1),
(3006,'SH-0007','凉感防晒衬衫','衬衫','URB','summer',219.00,1);

INSERT INTO fact_order VALUES
(50001,'2026-03-01',9001,3001,1,399.00,'app','浙江'),
(50002,'2026-03-01',9002,3002,2,258.00,'app','江苏'),
(50003,'2026-03-02',9003,3003,1,329.00,'tmall','上海'),
(50004,'2026-03-02',9004,3004,1,459.00,'jd','北京'),
(50005,'2026-03-03',9005,3006,1,219.00,'offline','广东'),
(50006,'2026-03-03',9006,3001,1,399.00,'app','浙江'),
(50007,'2026-03-04',9007,3002,1,129.00,'app','浙江'),
(50008,'2026-03-04',9008,3005,1,699.00,'tmall','四川');

INSERT INTO fact_review VALUES
(70001,50001,3001,5,'版型很正，通勤非常合适，基本不皱。','2026-03-01 10:20:00'),
(70002,50002,3002,4,'速干效果好，跑步穿着舒服。','2026-03-01 12:10:00'),
(70003,50003,3003,5,'上身很仙，约会很出片。','2026-03-02 14:00:00'),
(70004,50004,3004,3,'防泼水可以，但尺码略偏大。','2026-03-02 18:30:00'),
(70005,50005,3006,4,'夏天穿不闷，防晒通勤都行。','2026-03-03 09:40:00'),
(70006,50008,3005,2,'保暖可以，但感觉有点扎。','2026-03-04 20:00:00');
```

---

### 3.6 MCP 完整使用案例

#### 6.1 元数据探索（建模前）

| 工具                | 参数                                                                                            | 说明           |
| ------------------- | ----------------------------------------------------------------------------------------------- | -------------- |
| `get_db_list`       | `{"catalog_name": "internal"}`                                                                  | 获取数据库列表 |
| `get_db_table_list` | `{"db_name": "demo_doris_mcp_apparel", "catalog_name": "internal"}`                             | 获取表列表     |
| `get_table_schema`  | `{"db_name": "demo_doris_mcp_apparel", "table_name": "fact_order", "catalog_name": "internal"}` | 获取表结构     |

#### 6.2 经营看板查询（GMV/销量/客单价）

```json
{
  "db_name": "demo_doris_mcp_apparel",
  "catalog_name": "internal",
  "sql": "SELECT order_date, SUM(pay_amount) AS gmv, SUM(qty) AS sales_qty, ROUND(SUM(pay_amount)/COUNT(DISTINCT order_id),2) AS avg_order_amount FROM fact_order GROUP BY order_date ORDER BY order_date;"
}
```

#### 6.3 品类表现

```json
{
  "db_name": "demo_doris_mcp_apparel",
  "catalog_name": "internal",
  "sql": "SELECT p.category, SUM(o.pay_amount) AS gmv, SUM(o.qty) AS sales_qty FROM fact_order o JOIN dim_product p ON o.product_id=p.product_id GROUP BY p.category ORDER BY gmv DESC;"
}
```

#### 6.4 负向评价监控（评分 <= 3）

```json
{
  "db_name": "demo_doris_mcp_apparel",
  "catalog_name": "internal",
  "sql": "SELECT r.review_id, p.product_name, r.rating, r.review_text, r.review_time FROM fact_review r JOIN dim_product p ON r.product_id=p.product_id WHERE r.rating <= 3 ORDER BY r.review_time DESC;"
}
```

#### 6.5 SQL 优化闭环（Explain + 执行）

```json
{
  "db_name": "demo_doris_mcp_apparel",
  "catalog_name": "internal",
  "sql": "SELECT p.brand, SUM(o.pay_amount) AS gmv FROM fact_order o JOIN dim_product p ON o.product_id=p.product_id GROUP BY p.brand ORDER BY gmv DESC;",
  "verbose": true
}
```

#### 6.6 数据治理示例（质量分析）

```json
{
  "table_name": "fact_review",
  "analysis_scope": "full",
  "sample_size": 1000,
  "business_rules": ["rating BETWEEN 1 AND 5", "review_text IS NOT NULL"]
}
```

---

### 3.7 自然语言提问模板

- "请连接 `demo_doris_mcp_apparel`，列出所有表并说明主键字段。"
- "帮我查询最近每天 GMV、销量、客单价，并给出异常波动解释。"
- "找出评分小于等于 3 的评价，按产品聚合输出主要问题关键词。"
- "先 Explain 再执行品牌 GMV SQL，并给出可能的性能优化建议。"

---

### 3.8 验收标准

- [ ] `curl /health` 返回正常
- [ ] MCP 客户端可看到 Doris 工具列表（如 `exec_query`、`get_table_schema`）
- [ ] 至少 1 条元数据查询、2 条业务 SQL、1 条 explain/profile、1 条质量分析可成功返回

### 3.9 常见报错与处理

| 报错                  | 处理方式                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------ |
| 连接 Doris 失败       | 检查 `--db-host/--db-port` 是否对应 FE 查询端口（默认 9030），检查账号权限                 |
| 客户端连不上 MCP      | HTTP 模式确认 URL 为 `http://127.0.0.1:3000/mcp`，先用 `curl` 验证服务进程                 |
| 工具返回 SQL 安全拦截 | 查看服务端安全配置（如 `ENABLE_SECURITY_CHECK`、`BLOCKED_KEYWORDS`），先用只读查询验证链路 |

---

## 4. Doris AI Functions

### 4.1 支持的 AI 提供商

> OpenAI、Anthropic、Gemini、DeepSeek、Local、MoonShot、MiniMax、智谱、百川

### 4.2 应用场景

| 场景         | 说明                                     |
| ------------ | ---------------------------------------- |
| **智能反馈** | 自动识别用户意图和情绪                   |
| **内容审核** | 批量检测和处理敏感信息，以确保合规性     |
| **用户洞察** | 自动对用户反馈进行分类和总结             |
| **数据治理** | 通过智能纠错和关键信息提取来提高数据质量 |

---

### 4.3 AI 函数一览

| 函数            | 功能                                                 |
| --------------- | ---------------------------------------------------- |
| `AI_CLASSIFY`   | 从给定的标签中提取与文本内容最匹配的单个标签字符串   |
| `AI_EXTRACT`    | 根据文本内容提取每个给定标签的相关信息               |
| `AI_FILTER`     | 检查文本内容是否正确，并返回布尔值                   |
| `AI_FIXGRAMMAR` | 修正文本中的语法和拼写错误                           |
| `AI_GENERATE`   | 根据输入参数生成内容                                 |
| `AI_MASK`       | 根据标签替换原始文本中的敏感信息                     |
| `AI_SENTIMENT`  | 分析文本的情感，返回 positive/negative/neutral/mixed |
| `AI_SIMILARITY` | 确定两个文本之间的含义相似度（0~10）                 |
| `AI_SUMMARIZE`  | 提供文本的高度精炼摘要                               |
| `AI_TRANSLATE`  | 将文本翻译成指定的语言                               |
| `AI_AGG`        | 对多篇文本执行跨行聚合分析                           |

---

### 4.4 环境准备

```bash
mysql --default-character-set=utf8mb4 -uroot -P9030 -h192.168.63.128

SHOW RESOURCES;
SHOW CREATE RESOURCE "ai_deepseek";
SET NAMES utf8mb4;
```

#### 创建 AI Resource

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

-- 设默认资源（可选）
SET default_ai_resource = 'ai_deepseek';
```

---

### 4.5 测试数据准备

```sql
CREATE DATABASE IF NOT EXISTS demo_apparel_ai;
USE demo_apparel_ai;

-- 测试表：商品评价
CREATE TABLE IF NOT EXISTS apparel_reviews (
  review_id BIGINT,
  sku_id VARCHAR(32),
  user_id BIGINT,
  rating TINYINT,
  review_text STRING
)
DUPLICATE KEY(review_id)
DISTRIBUTED BY HASH(review_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO apparel_reviews VALUES
(1,'JK001',10001,5,'这件防晒衣面料很轻薄，通勤穿不闷，颜色也很正。'),
(2,'JK001',10002,2,'尺码偏小，袖口线头有点多，穿两次就起球。'),
(3,'DN778',10003,4,'牛仔裤版型不错，就是腰部略紧，建议拍大一码。'),
(4,'DN778',10004,1,'收到就有刺鼻味道，还掉色，把白T都染蓝了。'),
(5,'TS309',10005,5,'T恤很软，洗了三次没变形，性价比高。'),
(6,'TS309',10006,3,'物流慢了两天，客服回复还可以，衣服一般。');

-- 测试表：售后工单（含敏感信息）
CREATE TABLE IF NOT EXISTS apparel_tickets (
  ticket_id BIGINT,
  order_id VARCHAR(40),
  customer_name VARCHAR(50),
  phone VARCHAR(30),
  address_text STRING,
  ticket_text STRING
)
DUPLICATE KEY(ticket_id)
DISTRIBUTED BY HASH(ticket_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO apparel_tickets VALUES
(101,'ORD-20260228-001','张三','13800138000','上海市浦东新区XX路88号',
'我是张三，订单ORD-20260228-001买的JK001，袖口开线，麻烦换货，电话13800138000，地址上海市浦东新区XX路88号。'),
(102,'ORD-20260228-002','李四','13900139000','广州市天河区YY街16号',
'裤子掉色严重，第一次机洗就把其他衣服染了，请尽快处理。');

-- 测试表：文案草稿（故意有语病）
CREATE TABLE IF NOT EXISTS apparel_copy_drafts (
  draft_id BIGINT,
  sku_id VARCHAR(32),
  draft_text STRING,
  audience VARCHAR(50),
  selling_points STRING
)
DUPLICATE KEY(draft_id)
DISTRIBUTED BY HASH(draft_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO apparel_copy_drafts VALUES
(201,'JK001','这款防晒衣很轻薄并且穿着不闷热，适合很多场景去穿。',
'都市通勤女性','轻薄透气,UPF50+,可机洗'),
(202,'DN778','牛仔裤版型很好看但是腰有点紧，建议大家可以买大一码会更舒服。',
'18-30岁年轻人','显腿直,耐磨,百搭');
```

---

### 4.6 AI 函数示例

#### A. AI_CLASSIFY：评论分类

```sql
SELECT
  review_id,
  AI_CLASSIFY(
    review_text,
    ['尺码问题','面料体验','做工质量','物流服务','性价比']
  ) AS category
FROM apparel_reviews;
```

#### B. AI_EXTRACT：关键信息提取

```sql
SELECT
  review_id,
  AI_EXTRACT(
    review_text,
    ['颜色','尺码','面料','问题','使用场景']
  ) AS extracted_info
FROM apparel_reviews;
```

#### C. AI_FILTER：售后升级筛选

```sql
SELECT
  review_id, review_text
FROM apparel_reviews
WHERE AI_FILTER(
  CONCAT('请判断该评价是否需要售后升级处理（退换货/质量投诉），只按真假判断：', review_text)
);
```

#### D. AI_FIXGRAMMAR：修正文案病句

```sql
SELECT
  draft_id,
  AI_FIXGRAMMAR(draft_text) AS fixed_copy
FROM apparel_copy_drafts;
```

#### E. AI_GENERATE：自动生成营销短文案

```sql
SELECT
  draft_id,
  AI_GENERATE(
    CONCAT(
      '请写一条中文电商短文案，80字以内；人群：', audience,
      '；卖点：', selling_points,
      '；语气：真实、不过度夸张。'
    )
  ) AS gen_copy
FROM apparel_copy_drafts;
```

#### F. AI_MASK：敏感信息脱敏

```sql
SELECT
  ticket_id,
  AI_MASK(
    ticket_text,
    ['姓名','手机号','地址','订单号']
  ) AS masked_ticket
FROM apparel_tickets;
```

#### G. AI_SENTIMENT：情感分析

```sql
SELECT
  review_id,
  AI_SENTIMENT(review_text) AS sentiment
FROM apparel_reviews;
```

#### H. AI_SIMILARITY：语义相似度

```sql
SELECT
  review_id,
  review_text,
  AI_SIMILARITY('掉色严重，洗后串色，要求退货。', review_text) AS sim_score
FROM apparel_reviews
ORDER BY sim_score DESC;
```

#### I. AI_SUMMARIZE：工单摘要

```sql
SELECT
  ticket_id,
  AI_SUMMARIZE(ticket_text) AS ticket_summary
FROM apparel_tickets;
```

#### J. AI_TRANSLATE：多语言翻译

```sql
SELECT
  draft_id,
  AI_TRANSLATE(draft_text, 'en') AS draft_en
FROM apparel_copy_drafts;
```

#### K. AI_AGG：按 SKU 聚合评论洞察

```sql
SELECT
  sku_id,
  AI_AGG(
    review_text,
    '请把同一SKU的多条评价总结为一句中文：同时给出主要优点和主要缺点，50字以内。'
  ) AS sku_review_summary
FROM apparel_reviews
GROUP BY sku_id;
```

---

### 4.7 实战：把 AI 结果写回宽表

```sql
CREATE DATABASE IF NOT EXISTS demo_ai;
USE demo_ai;

-- 评论表
CREATE TABLE IF NOT EXISTS apparel_reviews (
  review_id BIGINT,
  order_id BIGINT,
  sku_id VARCHAR(64),
  channel VARCHAR(32),
  rating INT,
  review_text TEXT,
  created_at DATETIME
)
DUPLICATE KEY(review_id)
DISTRIBUTED BY HASH(review_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 测试数据
INSERT INTO apparel_reviews VALUES
(1, 10001, 'SKU-TSHIRT-001', 'app', 5, '上身很好看，面料舒服，物流也快，会回购。', '2026-02-20 10:10:00'),
(2, 10002, 'SKU-TSHIRT-001', 'app', 2, '尺码偏小，洗一次就有点变形，不太满意。', '2026-02-20 11:20:00'),
(3, 10003, 'SKU-JEANS-003', 'tmall', 4, '版型不错 but the zipper feels cheap.', '2026-02-20 12:00:00'),
(4, 10004, 'SKU-DRESS-009', 'jd', 1, '颜色和图片差很多，客服回复慢，退货体验差。', '2026-02-20 13:10:00'),
(5, 10005, 'SKU-SHOES-006', 'offline', 5, '店员服务专业，试穿体验很好。', '2026-02-20 14:25:00'),
(6, 10006, 'SKU-COAT-002', 'app', 3, '保暖还行，线头较多，整体一般。', '2026-02-20 15:40:00');

-- AI 分析结果表
CREATE TABLE IF NOT EXISTS apparel_reviews_ai (
  review_id BIGINT,
  sentiment VARCHAR(32),
  label VARCHAR(64),
  summary TEXT,
  service_reply TEXT
)
DUPLICATE KEY(review_id)
DISTRIBUTED BY HASH(review_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 写入 AI 分析结果
INSERT INTO apparel_reviews_ai
SELECT
  review_id,
  AI_SENTIMENT(review_text) AS sentiment,
  AI_CLASSIFY(review_text, ['size_issue','quality_issue','logistics','service','positive_feedback']) AS label,
  AI_GENERATE(CONCAT('请用中文总结以下评价，20字以内，仅输出总结本身：', review_text)) AS summary,
  AI_GENERATE(CONCAT('请给出客服回复建议，控制在80字以内：', review_text)) AS service_reply
FROM apparel_reviews;
```

---

### 4.8 补充说明

> **注意**：
>
> - AI 函数结果是大模型生成，非固定值，每次可能略有差异
> - 成本与时延取决于你配置的外部模型服务
> - 建议先在测试库小批量跑，再上生产任务

---

## 5. Flink AI 功能

### 5.1 目标

- 聊天完成（Chat Completion）：自动生成客服标准回复
- 嵌入（Embedding）：生成商品文案向量，用于检索/相似推荐
- 同时给出 Flink SQL 与 Flink API（Table API）两种实现

### 5.2 场景

- 电商服装品牌售后中心
- 输入是用户咨询、商品描述
- 输出是客服建议话术 + 向量特征（用于后续向量库或召回模型）

---

### 5.3 环境与前置条件

| 项目       | 要求                                                           |
| ---------- | -------------------------------------------------------------- |
| Flink 版本 | 2.2.x（SQL Client 或 Flink 集群）                              |
| JDK        | 17+                                                            |
| 依赖       | Flink OpenAI Model Connector（官方文档 Download 页面提供 jar） |
| 模型服务   | Flink 通过 OpenAI 兼容接口调用远程模型服务                     |

[https://repo.maven.apache.org/maven2/org/apache/flink/flink-model-openai/2.2.0/flink-model-openai-2.2.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-model-openai/2.2.0/flink-model-openai-2.2.0.jar)

> **密钥管理建议**：不要把 API Key 明文写入 Git，建议使用环境变量或配置中心注入，例如 `${DEEPSEEK_API_KEY}`

---

### 5.4 测试数据

#### 客服咨询测试数据

```sql
CREATE TEMPORARY VIEW apparel_service_chat_input AS
SELECT * FROM (
  VALUES
    ('T1001', '我买的防晒衬衫洗后有点皱，能不能给下护理建议？'),
    ('T1002', '牛仔裤尺码偏小，想换大一码，怎么操作？'),
    ('T1003', '外套物流延迟了两天，能否催一下？')
) AS t(ticket_id, customer_msg);
```

#### 商品文案测试数据

```sql
CREATE TEMPORARY VIEW apparel_product_desc_input AS
SELECT * FROM (
  VALUES
    ('SKU-TS-001', '凉感速干T恤，适合夏季通勤，面料轻薄透气'),
    ('SKU-JK-008', '防泼水冲锋外套，适合春秋通勤与户外'),
    ('SKU-DR-021', '法式碎花连衣裙，轻盈垂坠，适合约会与出游')
) AS t(sku_id, product_desc);
```

---

### 5.5 Flink SQL 实现

#### 创建聊天模型（DeepSeek）

```sql
DROP MODEL IF EXISTS apparel_chat_model;
CREATE MODEL apparel_chat_model
INPUT (message STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'openai',
  'endpoint' = 'https://api.openai.com/v1/chat/completions',
  'api-key' = '',
  'model' = 'gpt-3.5-turbo',
  'system-prompt' = '你是服装品牌客服助手。请基于用户问题输出标准回复，语气专业友好，60字以内，包含可执行下一步。'
);
```

#### 创建嵌入模型

```sql
DROP MODEL IF EXISTS apparel_embed_model;
CREATE MODEL apparel_embed_model
INPUT (text STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider' = 'openai',
  'endpoint' = 'https://api.openai.com/v1/embeddings',
  'api-key' = '',
  'model' = 'embeddings'
);
```

#### 聊天完成推理（标准客服回复）

```sql
CREATE TEMPORARY TABLE apparel_chat_reply_sink (
  ticket_id STRING,
  customer_msg STRING,
  service_reply STRING
) WITH ('connector' = 'print');

INSERT INTO apparel_chat_reply_sink
SELECT
  ticket_id,
  customer_msg,
  response AS service_reply
FROM ML_PREDICT(
  INPUT  => TABLE apparel_service_chat_input,
  MODEL  => MODEL apparel_chat_model,
  ARGS   => DESCRIPTOR(customer_msg)
);
```

#### 嵌入推理（向量特征）

```sql
CREATE TEMPORARY TABLE apparel_embedding_sink (
  sku_id STRING,
  product_desc STRING,
  embedding ARRAY<FLOAT>
) WITH ('connector' = 'print');

INSERT INTO apparel_embedding_sink
SELECT
  sku_id,
  product_desc,
  embedding
FROM ML_PREDICT(
  INPUT  => TABLE apparel_product_desc_input,
  MODEL  => MODEL apparel_embed_model,
  ARGS   => DESCRIPTOR(product_desc)
);
```

---

### 5.6 Flink API（Table API）实现

> 适用场景：需要在 Java 作业中把 AI 推理和业务逻辑、维表 Join、窗口聚合一起编排。

```java
package com.demo.apparel;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.ml.Model;
import org.apache.flink.table.ml.ModelDescriptor;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.map;

public class ApparelAiJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1) 构造测试数据表（客服）
        Table chatInput = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("ticket_id", DataTypes.STRING()),
                        DataTypes.FIELD("customer_msg", DataTypes.STRING())
                ),
                Row.of("T1001", "我买的防晒衬衫洗后有点皱，能不能给下护理建议？"),
                Row.of("T1002", "牛仔裤尺码偏小，想换大一码，怎么操作？")
        );
        tEnv.createTemporaryView("chat_input", chatInput);

        // 2) 创建 Chat Model（DeepSeek）
        tEnv.createModel(
                "apparel_chat_model_api",
                ModelDescriptor.forProvider("openai")
                        .option("task", "chat")
                        .option("endpoint", "https://api.deepseek.com/v1/chat/completions")
                        .option("api-key", System.getenv("DEEPSEEK_API_KEY"))
                        .option("model", "deepseek-chat")
                        .inputSchema(Schema.newBuilder().column("message", DataTypes.STRING()).build())
                        .outputSchema(Schema.newBuilder().column("response", DataTypes.STRING()).build())
                        .build()
        );

        // 3) 执行聊天推理
        Table chatResult = tEnv.from("chat_input")
                .select(
                        $("ticket_id"),
                        $("customer_msg"),
                        Model.predict(
                                "apparel_chat_model_api",
                                $("customer_msg"),
                                map("prompt", "你是服装品牌客服助手，请输出60字以内专业回复，并附下一步处理建议。")
                        ).as("service_reply")
                );

        tEnv.createTemporaryView("chat_result", chatResult);
        tEnv.executeSql("CREATE TEMPORARY TABLE print_chat (ticket_id STRING, customer_msg STRING, service_reply STRING) WITH ('connector'='print')");
        chatResult.executeInsert("print_chat");

        // 4) 创建 Embedding Model
        tEnv.createModel(
                "apparel_embed_model_api",
                ModelDescriptor.forProvider("openai")
                        .option("task", "embed")
                        .option("endpoint", "https://api.deepseek.com/v1/embeddings")
                        .option("api-key", System.getenv("DEEPSEEK_API_KEY"))
                        .option("model", "deepseek-embedding")
                        .inputSchema(Schema.newBuilder().column("text", DataTypes.STRING()).build())
                        .outputSchema(Schema.newBuilder().column("embedding", DataTypes.ARRAY(DataTypes.DOUBLE())).build())
                        .build()
        );

        Table embedInput = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("sku_id", DataTypes.STRING()),
                        DataTypes.FIELD("product_desc", DataTypes.STRING())
                ),
                Row.of("SKU-TS-001", "凉感速干T恤，适合夏季通勤，面料轻薄透气"),
                Row.of("SKU-JK-008", "防泼水冲锋外套，适合春秋通勤与户外")
        );

        Table embedResult = embedInput.select(
                $("sku_id"),
                $("product_desc"),
                Model.predict("apparel_embed_model_api", $("product_desc")).as("embedding")
        );

        tEnv.executeSql("CREATE TEMPORARY TABLE print_embed (sku_id STRING, product_desc STRING, embedding ARRAY<DOUBLE>) WITH ('connector'='print')");
        embedResult.executeInsert("print_embed");
    }
}
```

---

### 5.7 与实时湖仓链路集成建议

#### 推荐链路

```
MySQL/OMS/POS → Flink CDC → Kafka → Flink SQL/API(AI推理) → Paimon/Iceberg → Doris/ES
```

#### 典型落地

| 输出           | 用途                                                               |
| -------------- | ------------------------------------------------------------------ |
| Chat 输出      | 写入客服工单宽表，用于质检与客服 SOP                               |
| Embedding 输出 | 写入向量检索库（或 Doris 向量列），用于"相似商品推荐/相似投诉聚类" |

#### 宽表示例字段

```
ticket_id, sku_id, customer_msg, ai_reply, ai_sentiment, ai_embedding, proc_time
```

---

### 5.8 验收标准

- [ ] **聊天完成**：`apparel_chat_reply_sink` 打印出每条咨询对应 `service_reply`，回复语气符合客服规范且给出下一步动作
- [ ] **嵌入**：`apparel_embedding_sink` 打印出 `ARRAY<DOUBLE>` 向量，相近文本向量距离应更接近
- [ ] **稳定性**：API Key 缺失、限流、超时有明确重试或告警策略，先小流量验证，再扩大到生产全量

---

### 5.9 常见问题

| 问题                                | 解决方案                                               |
| ----------------------------------- | ------------------------------------------------------ |
| `ML_PREDICT` 报 endpoint/model 错误 | 检查是否为 OpenAI 兼容接口路径，检查模型名称是否已开通 |
| 返回速度慢或失败率高                | 增加重试、并发控制和超时参数，结合业务做异步缓冲与降级 |
| 向量维度不一致                      | 不同 embedding 模型维度不同，落库前要统一 schema       |

---

## 文档信息

| 项目     | 内容                       |
| -------- | -------------------------- |
| 文档标题 | 实时湖仓技术栈 AI 增强实践 |
| 适用场景 | 电商服装行业数据湖仓       |
| 核心技术 | Doris、Flink               |
