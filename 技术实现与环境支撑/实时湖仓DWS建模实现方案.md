# 实时湖仓DWS建模实现方案

> 以下为对应 SQL 脚本的完整内容，已原样搬运，便于在仅支持 Markdown 的设备中查看。
> 源文件：实时湖仓DWS建模实现方案.sql

```sql
/********************************************************************************
 * 实时湖仓DWS建模解读
 * DWS层(Data Warehouse Service): 又称"公共汇总层", 其目标是提炼指标, 为下游应用提供高性能的查询支持
 *
 * 职责: 覆盖80%的通用查询场景, 通过减少重复计算来提升查询效率并降低存储压力
 *
 * 核心操作:
 * 轻度聚合: 基于DWD层的明细数据, 按照常见维度(如时间天级/小时级、地域、用户分组)进行预聚合计算
 * 主题域划分: 以业务主体为中心构建宽表, 如"用户主题宽表"(包含用户近期的累计消费金额、活跃天数等)或"商品主题宽表"
 * 多维汇总: 通过Flink的Window等, 实时计算GMV、UV/PV等关键业务指标并沉淀在湖仓中
 *
 * DWS层生产级建模规范: 三大原则
 * 1. 维度建模与公共粒度(Common Grain)
 * 规范: DWS层不再存储明细, 而是按主题(如销售、库存、退货)进行多维聚合
 * 原则: 确定"公共粒度", 例如, 服装行业最常见的公共粒度是:[日期+门店+SKU]或[小时+区域+品类]
 * 目的: 减少重复计算, 下游80%的报表应能通过直接查询DWS表获得结果
 *
 * 2. 度量口径一致性(Consistent Metrics)
 * 规范: 所有核心指标(如: GMV、件单价、售罄率、售罄比)必须在DWS层完成计算公式的固化
 * 目的: 避免"同一个指标在不同报表里数据对不上"的问题
 *
 * 3. 灵活的聚合策略(Rolling Aggregation)
 * 规范: 区分"加和性指标"(如销量)和"非加和性指标"(如去重客流、库存余量)
 * 实践: 利用Doris的Aggregate模型自动处理数据合并
 *
 * 工业级建模方法: 湖仓一体的汇总策略
 * DWS的构建通常分为"实时预计算"和"即席联邦聚合"两种方式:
 * 1. 预聚合汇总(Pre-Aggregation)
 * 方法: Flink消费DWD, 按分钟/小时进行聚合, 写入Doris的Aggregate模型表
 * 适用场景: 对响应时间要求极高(毫秒级)的固定报表, 如"双11实时全国总销售额"
 *
 * 2. 读时聚合/外表加速(On-the-fly Aggregation)
 * 方法: Doris通过Catalog直接映射Paimon的DWD明细表
 * 适用场景: 灵活的探索性分析, 业务方可能随时更改筛选维度
 *
 * 利用Doris物化视图(MV)加速查询性能: Doris的异步物化视图(Async Materialized View)是目前解决湖仓分析延迟的最佳实践!
 *
 * 为什么用物化视图？
 * 透明改写: 业务方查询的是Paimon DWD明细视图, 但Doris优化器会自动发现已经预计算好的物化视图, 并自动路由,
            查询耗时从10秒降到50 毫秒
 * 自动刷新: 支持基于Paimon增量数据的定时刷新(例如每分钟刷新一次), 保证数据的准实时性
 *
 * 核心加速策略?
 * 降维加速: 基于DWD宽表, 预先计算好[季节 + 品类]粒度的销售额
 * 去重加速: 利用Doris内置的Bitmap或HLL算法, 在物化视图中预计算UV(独立访客)等去重指标
 *
 * DWD与DWS的主要区别
 * 特性 	    DWD层(Data Warehouse Detail)	DWS层(Data Warehouse Service)
 * 数据粒度	    最细粒度(每一笔交易、每一次点击)	  聚合粒度(每日汇总、每小时统计)
 * 主要目标	    保证数据质量、实现维度对齐	         提升查询速度、指标复用
 * 表现形式	    明细宽表或标准事实表	            以业务主题划分的公共汇总宽表
 * 典型案例	    实时订单明细表	                   实时店铺当日销量汇总表
 *
 * DWD层解决了数据的"规范与准确", 而DWS层解决了数据的"性能与效率"
 * 两者结合使得企业既能追溯实时发生的每一个细节, 也能瞬间获取全局的统计视图
 *
 * 业务背景:
 * 在服装行业,DWS层的目标是支撑实时战报(如:今日全国各季节品类销冠)和经营分析(如:售罄率趋势)
 * 我们将通过"预聚合汇总"和"读时聚合/外表加速"两种工业级方法来实现
 ********************************************************************************/

/********************************************************************************
 *步骤1: 创建Paimon外表Catalog
 湖仓身份转换:
 在执行完上面的脚本后, dwd_sales_detail在Doris里的身份是"External Table"(外表)
 它的优势是零数据拷贝, 劣势是如果直接在大规模数据上做COUNT(DISTINCT)聚合, 性能受限于HDFS的扫描速度
 Schema自动映射:
 Doris的Paimon Catalog会自动将Paimon的DECIMAL、TIMESTAMP等类型映射为Doris对应的强类型
 这为我们后续在Doris中直接创建基于外表的异步物化视图, 扫清了类型转换的障碍
 ********************************************************************************/

/*
 * 1. 创建Paimon外表Catalog
 */
CREATE CATALOG paimon_fashion_dw PROPERTIES (
    "type" = "paimon",
    "warehouse" = "hdfs://192.168.63.128:8020/paimon/fashion_dw",
    "fs.defaultFS" = "hdfs://192.168.63.128:8020",
    "hadoop.username" = "hdfs"
);
SHOW CATALOGS;
-- 切换到该Catalog验证数据
USE paimon_fashion_dw.dwd;
SWITCH paimon_fashion_dw;
SELECT CURRENT_CATALOG();
-- 查看数据库和表
SHOW DATABASES;
USE ods;
SHOW tables;

/*
 * 2. DWD挂载后的关键操作: 元数据刷新与手动验证
 * 由于Paimon的文件是Flink实时写入的, Doris默认会有一定的元数据缓存。在生产环境中, 我们需要确保元数据的可见性
 */
-- 强制刷新Catalog 元数据(确保能看到Flink刚写进去的最新分区和数据)
REFRESH CATALOG paimon_fashion_dw;

--  挂载验证: 查询我们在DWD层做好的明细宽表
SELECT
    order_id,
    sku_name,
    season,
    order_amount,
    order_ts
FROM dwd_sales_detail
ORDER BY order_ts DESC
LIMIT 10;

/********************************************************************************
 * 步骤2: DWS层建模设计方案
 * 按照"三大原则(公共粒度、口径一致、聚合策略)", 我们设计以下两个模型:
 * 模型A(预聚合汇总): dws_sales_org_sku_1h
 * 方法: 物理存储在Doris内部,采用Aggregate模型
 * 粒度: [按小时+门店+SKU]
 * 价值: 极致性能,支撑秒级高频刷新的实时大屏
 *
 * 模型B(读时聚合/外表加速): dws_season_performance_view
 * 方法: 异步物化视图(Async MV)
 * 粒度: [按天+季节+品类]
 * 价值: 逻辑解耦, 自动路由加速, 无需维护复杂的ETL任务
 ********************************************************************************/

/********************************************************************************
 * 步骤3: DWS层建模开发
 ********************************************************************************/

/*
 * 1. 预聚合汇总实现(Doris Aggregate Model)
 * 首先, 在Doris本地库中创建物理汇总表, Doris的AGGREGATE KEY引擎会在写入时自动进行累加
 */
-- 切换到Doris内部存储
SWITCH internal;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

-- 创建DWS物理汇总表
CREATE TABLE dws_sales_org_sku_1h (
    event_hour DATETIME,           -- 公共粒度: 小时
    sku_id INT,                    -- 公共粒度: SKU
    sku_name VARCHAR(100),         -- 冗余维度
    season VARCHAR(20),            -- 冗余维度
    total_amount DECIMAL(18, 2) SUM, -- 度量: 销售额累加
    order_count BIGINT SUM         -- 度量: 订单数累加
)
AGGREGATE KEY(event_hour, sku_id, sku_name, season)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1" -- 测试环境设为1
);

-- ETL 脚本: 从Paimon DWD抽取并预聚合(生产中通常由Flink执行或Doris定时任务)
INSERT INTO dws_sales_org_sku_1h
SELECT
    DATE_FORMAT(order_ts, '%Y-%m-%d %H:00:00') as event_hour,
    sku_id,
    sku_name,
    season,
    order_amount,
    1 as order_count
FROM paimon_fashion_dw.dwd.dwd_sales_detail;

/*
 * 2. 读时聚合/外表加速实现 (Async Materialized View)
 * 利用Doris3.0+的杀手锏功能, 直接在外表上构建异步物化视图
 */
-- 创建异步物化视图(针对服装季节分析主题)
CREATE MATERIALIZED VIEW mv_season_category_sales_daily
BUILD IMMEDIATE
REFRESH AUTO                           -- 或者 COMPLETE
ON SCHEDULE EVERY 1 MINUTE             -- 使用ON SCHEDULE定义时间间隔, 每分钟自动增量刷新
DISTRIBUTED BY HASH(season) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"            -- 建议显式指定副本数, 根据集群实际情况调整
)
AS
SELECT
    CAST(order_ts AS DATE) as sales_date,
    season,
    COUNT(DISTINCT order_id) as uv_orders, -- 读时去重
    SUM(order_amount) as total_revenue
FROM paimon_fashion_dw.dwd.dwd_sales_detail
GROUP BY sales_date, season;

/********************************************************************************
 * 步骤4: 执行操作与结果验证
 ********************************************************************************/

/*
 * 1. 验证预聚合汇总(模型A)
 * 注意⚠️: 需要多模拟一个批次数据, 不然DWD层不会触发写入
 * 动作: 在Paimon DWD层产生10条同一小时、同一SKU的订单
 */
-- 模拟商品基础数据插入（辅助DWD层维度关联）
INSERT INTO goods_base (sku_id, sku_name, color) VALUES (103, '休闲工装长裤', '卡其灰');
INSERT INTO goods_price (sku_id, tag_price) VALUES (103, 199.00);
INSERT INTO goods_attr (sku_id, season) VALUES (103, '2025秋');

INSERT INTO goods_base (sku_id, sku_name, color) VALUES (104, '短款通勤羽绒服', '卡其灰');
INSERT INTO goods_price (sku_id, tag_price) VALUES (104, 699.00);
INSERT INTO goods_attr (sku_id, season) VALUES (104, '2025冬');

-- 模拟Kafka消息数据（发送到对应Topic）
/********************************************************************************
# 模拟发送到Kafka topic: fashion_app_click(点击流)
{"uid":"U9888","item_id":"103","action":"click","event_ts":"2025-12-26 14:00:00","ext_info":"{\"promo_id\":\"7\",\"client_type\":\"Android\",\"page\":\"home\",\"spm\":\"x.y.z\"}"}
{"uid":"U9888","item_id":"103","action":"click","event_ts":"2025-12-26 14:17:00","ext_info":"{\"promo_id\":\"7\",\"client_type\":\"Android\",\"page\":\"home\",\"spm\":\"x.y.z\"}"}

# 模拟发送到Kafka topic: fashion_app_order(订单流)
{"oid":"ORD_20251224_003","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:00:00"}
{"oid":"ORD_20251224_004","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:00:30"}
{"oid":"ORD_20251224_005","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:01:00"}
{"oid":"ORD_20251224_006","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:01:30"}
{"oid":"ORD_20251224_007","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:02:00"}
{"oid":"ORD_20251224_008","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:02:30"}
{"oid":"ORD_20251224_009","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:03:00"}
{"oid":"ORD_20251224_010","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:03:30"}
{"oid":"ORD_20251224_011","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:04:00"}
{"oid":"ORD_20251224_012","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:04:30"}
{"oid":"ORD_20251224_013","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:05:30"}
{"oid":"ORD_20251224_013","uid":"U9888","item_id":"103","amt":199.00,"order_ts":"2025-12-26 14:17:30"}
********************************************************************************/

/*
 * 执行: 重新插入数据到DWS预聚合表
 */
INSERT INTO dws_sales_org_sku_1h
SELECT
    DATE_FORMAT(order_ts, '%Y-%m-%d %H:00:00') as event_hour,
    sku_id,
    sku_name,
    season,
    order_amount,
    1 as order_count
FROM paimon_fashion_dw.dwd.dwd_sales_detail;

/*
 * 查询: 验证聚合结果
 */
SELECT * FROM dws_sales_org_sku_1h;

/*
 * 预期: 你会发现Doris物理表中只有1行数据, 金额和数量已经自动完成累加
 */

/*
 * 2. 验证物化视图加速(模型B)
 * 动作: 直接查询Paimon DWD明细表
 */
-- 即使你查询原始明细表, Doris也会自动路由到MV
SELECT CAST(order_ts AS DATE), season, SUM(order_amount)
FROM paimon_fashion_dw.dwd.dwd_sales_detail
GROUP BY 1, 2;

/*
 * 执行计划验证: 使用EXPLAIN ... 查看SQL执行计划
 */
EXPLAIN
SELECT CAST(order_ts AS DATE), season, SUM(order_amount)
FROM paimon_fashion_dw.dwd.dwd_sales_detail
GROUP BY 1, 2;

/*
 * 预期: 在Query Plan中能看到mv_season_category_sales_daily字样, 说明透明改写成功, 查询速度比直接扫Paimon快一个量级
 */

/********************************************************************************
 * 步骤5: 工业级建模方法总结
 * DWS层的"降维"思想:
 * 在服装行业, SKU级数据量巨大。 通过DWS层将数据降维到"品类/季节"级别, 能让90%的管理层报表不再直接扫描明细, 极大地保护了集群稳定性
 *
 * Aggregate模型 vs 物化视图:
 * 模型A(Aggregate)是"主动式"的: 你需要写ETL任务去喂数据, 适合核心、高频的固定指标
 * 模型B(MV)是"声明式"的: 你定义好规则, Doris自动维护, 适合多维、易变的分析需求
 *
 * 口径一致性控制:
 * 所有DWS表的total_amount必须包含"退货金额抵扣"逻辑(如果在DWD层已处理), 这样下游取数才不会乱
 ********************************************************************************/

/********************************************************************************
 * 步骤6: 重点关注
 * 以下三个"灵魂细节", 这会极大加深您对实时湖仓架构的理解:
 * 1. 观察聚合发生的时机:
 *    在操作Aggregate模型表时, 您可以先插入两条明细, 然后立即 SELECT, 您会发现, 哪怕这两条数据在逻辑上是先后进入的, 由于它们的AGGREGATE KEY相同, Doris会在后台(或查询时)瞬间完成合并, 这种"写入即计算"的快感是传统数仓无法比拟的
 *
 * 2. 感受"透明改写"的魔力:
 *    在物化视图(MV)构建成功并完成首次刷新后, 请务必尝试用EXPLAIN命令查看您针对Paimon外表的查询, 当您看到执行计划自动从paimon_table切换到了mv_table 时, 您就能体会到为什么都推崇"读写分离+视图加速"
 *
 * 3. 处理"空值"对聚合的影响:
 *    服装行业的SKU属性(如季节、品类)有时在DWD层可能关联失败为NULL, 在DWS聚合时, 观察这些NULL是如何归类的, 在生产规范中,
 *    通常建议在进入DWS前给NULL分配一个Unknown标签, 以保证报表的美观
 ********************************************************************************/
/*
 * 实战贴士:
 * 如果遇到异步物化视图不自动刷新的情况, 可以通过以下命令手动触发一次刷新, 便于快速看到效果
 */
-- 手动刷新物化视图
REFRESH MATERIALIZED VIEW mv_season_category_sales_daily AUTO;
REFRESH MATERIALIZED VIEW mv_season_category_sales_daily COMPLETE;
-- 查看物化视图的任务状态
SELECT * FROM jobs("type"="mv") WHERE name="mv_season_category_sales_daily";
-- 或者查看所有物化视图任务记录
SHOW MTMV JOB;

/********************************************************************************
 * 评估Doris Paimon外表(DWD)和Doris建表+Paimon Doris外表写入(双写)两种方案
 * 1. 方案可行性评估
 * 结论: 完全可行, 且在追求极致查询性能的场景下非常稳健, 本质上是将Doris作为Serving Layer(服务层),
 *      通过物理同步, 将计算压力从HDFS转移到了Doris的计算节点上
 *
 * 优点:
 * 查询速度上限更高: 物理存在Doris里的数据拥有索引加速(前缀索引、Bloom Filter)和分区分桶裁剪, 性能远超直接查询Paimon外表
 * 物化视图更灵活: 基于Doris本地表的物化视图(MV)支持同步刷新, 而基于外表的MV往往只能异步刷新
 *
 * 挑战:
 * 链路冗余: 增加了一层同步任务, 需监控Paimon到Doris的数据一致性
 * 存储成本: 同一份DWD数据在HDFS(Paimon)和Doris(本地存储)中各存了一份
 *
 * 2. 方案对比: 您的方案 vs Paimon Catalog直连方案
 * 维度       Paimon同步到Doris本地                     Doris直接读Paimon
 * 查询性能    极高, 享受Doris本地索引与向量化加速          中等, 受限于HDFS IO和Parquet/ORC解码
 * 数据实时性  取决于同步任务(通常有10s级延迟)              极高, Paimon写入成功, Doris刷新即见
 * 开发复杂度  略高, 需要维护Paimon到Doris的写入Job        极低, 只需创建一个Catalog即可
 * 适用场景    高并发、亚秒级响应的固定报表与大屏            中低频、探索性的Ad-hoc报表分析
 * 物化视图    支持同步与异步物化视图, 稳定性极佳            仅支持异步物化视图
 *
 * 3. 企业的生产级建议
 * 在大型服装企业(尤其是应对双11大促时), 推荐采用【Paimon同步到Doris本地】的方案, 但需要进行以下工业级优化:
 * 1) 采用Flink双写(而不是Doris挂载Catalog同步)
 * 建议: 在Flink DWD任务中, 利用STATEMENT SET将数据同时写入Paimon和Doris
 * 理由:
 * 原子性: 一份Checkpoint覆盖两个Sink, 保证数据水位一致
 * 低延迟: 不需要先入湖再入库, 数据同时到达湖和仓
 *        Catalog选型: 在Flink中创建Doris Catalog, 直接定义Doris表
 *
 * 2) Doris DWD表模型选型: Unique Key vs Duplicate Key
 * 服装场景建议:
 * 如果是订单明细, 使用Unique Key模型, 它可以防止因Flink重启导致的重复订单进入Doris, 实现物理去重
 * 如果是埋点点击流, 使用Duplicate Key模型, 追求写入的高吞吐
 *
 * 3) 利用Doris物化视图做DWS层的"降维打击"
 * 实践: 在Doris本地DWD表之上, 建立同步物化视图
 * 优势: 当订单数据进入Doris DWD的那一刻, Doris内部会自动增量更新DWS维度的聚合值(如门店销售总额),
 *      这种"写入即聚合"的模式比异步MV响应更快
 *
 * 4) 存储生命周期管理(TTL)
 * 建议: Doris侧只保留近3-6个月的热数据用于报表; Paimon侧保留全量历史数据
 * 理由: 服装行业季节性强, 去年的数据很少需要亚秒级响应, 存放在 HDFS(Paimon)中更经济
 *
 * 4. 生产级模拟实战建议:
 * 4.1 Paimon同步到Doris本地
 * 在Doris中创建本地DWD表(遵循Unique Key模型,对齐Paimon)
 * 通过Flink SQL实现数据从Paimon到Doris的同步(或模拟双写)
 * 在Doris本地DWD表上构建DWS聚合视图
 *
 * 4.2 基于Doris异步物化视图+Bitmap预计算的生产级实现方案
 * 异步物化视图加速: "售罄分析"和"时段趋势"
 * 作业: 整理一份关于"查询耗时对比"和"物化视图刷新成功率"的总结
 ********************************************************************************/

```

---

## 基于已验证 DWD 的生产版 DWS 建设

> 说明：以下内容基于当前已经验证通过的 DWD 实现继续向下建设，目标是在 Doris 中沉淀 DWS 公共汇总层，并通过 Doris 外部表访问 Paimon 明细表。
>
> 依赖的 DWD 表：
>
> - `dwd_order_trade_detail_rt`
> - `dwd_inventory_flow_detail_rt`
> - `dwd_logistics_track_detail_rt`
> - `dwd_user_behavior_detail_rt`
> - `dwd_social_mention_detail_rt`
> - `dwd_hot_sku_event_detail_rt`

## 一、DWS 建设目标

围绕两大核心场景，在 Doris 中沉淀可直接服务看板、接口与专题分析的公共汇总层：

1. 全渠道实时库存可视与智能调拨
2. 爆款趋势识别与快速追单

### 设计原则

- 公共粒度优先：先定义粒度，再定义指标
- 口径统一：所有核心指标在 DWS 固化
- 热点指标实时化：由 Flink SQL 实时预聚合写入 Doris
- 探索分析外表化：通过 Doris 外部表访问 Paimon DWD 明细
- 主题与场景对应：库存主题、爆款主题分别独立建设

## 二、DWS 目标表设计

| 场景           | DWS 表                      | 粒度                        | 核心指标                                             |
| -------------- | --------------------------- | --------------------------- | ---------------------------------------------------- |
| 库存可视与调拨 | `dws_inventory_atp_rt`      | 分钟 + 店仓 + SKU           | 可用库存、锁定库存、在途库存、ATP                    |
| 库存可视与调拨 | `dws_inventory_dispatch_rt` | 分钟 + 店仓 + SKU           | 缺货风险、订单需求、调拨建议                         |
| 爆款趋势识别   | `dws_hot_sku_feature_rt`    | 小时 + SKU                  | 销售额、订单数、浏览、点击、加购、社媒热度、爆款指数 |
| 爆款趋势识别   | `dws_hot_sku_rank_rt`       | 小时 + 品牌/季节/品类 + SKU | 热度排名、追单建议标签                               |

## 三、Doris 外部表访问 Paimon 明细

> 该部分在 Doris 中执行，用于直接访问 `erp_dw` 下的 Paimon DIM / DWD 表，为排查和探索分析提供基础。

```sql
-- Doris SQL
CREATE CATALOG paimon_erp_dw PROPERTIES (
    "type" = "paimon",
    "warehouse" = "hdfs://192.168.63.128:8020/paimon/erp_dw",
    "fs.defaultFS" = "hdfs://192.168.63.128:8020",
    "hadoop.username" = "hdfs"
);

REFRESH CATALOG paimon_erp_dw;

SWITCH paimon_erp_dw;
USE dwd;

-- 验证 DWD 明细
SELECT * FROM dwd_order_trade_detail_rt LIMIT 10;
SELECT * FROM dwd_inventory_flow_detail_rt LIMIT 10;
SELECT * FROM dwd_hot_sku_event_detail_rt LIMIT 10;
```

## 四、Doris 内部 DWS 目标表

> 该部分在 Doris 中执行，Flink SQL 后续实时写入这些表。

```sql
-- Doris SQL
SWITCH internal;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

CREATE TABLE dws_inventory_atp_rt (
    stat_minute DATETIME,
    store_id VARCHAR(32),
    warehouse_id VARCHAR(32),
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    available_qty DECIMAL(18, 2) SUM,
    locked_qty DECIMAL(18, 2) SUM,
    in_transit_qty DECIMAL(18, 2) SUM,
    stock_delta_qty DECIMAL(18, 2) SUM,
    atp_qty DECIMAL(18, 2) SUM
)
AGGREGATE KEY(stat_minute, store_id, warehouse_id, sku_id, spu_code, sku_name, brand_name, season, product_type)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE dws_inventory_dispatch_rt (
    stat_minute DATETIME,
    store_id VARCHAR(32),
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    atp_qty DECIMAL(18, 2) SUM,
    order_qty DECIMAL(18, 2) SUM,
    order_amount DECIMAL(18, 2) SUM,
    shortage_risk_flag TINYINT MAX,
    dispatch_priority_score DECIMAL(18, 4) MAX
)
AGGREGATE KEY(stat_minute, store_id, sku_id, spu_code, sku_name, brand_name, season, product_type)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE dws_hot_sku_feature_rt (
    stat_hour DATETIME,
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    wave_band VARCHAR(64),
    series VARCHAR(64),
    product_type VARCHAR(64),
    sale_amount_1h DECIMAL(18, 2) SUM,
    order_cnt_1h BIGINT SUM,
    view_cnt_1h BIGINT SUM,
    click_cnt_1h BIGINT SUM,
    add_cart_cnt_1h BIGINT SUM,
    social_hot_1h DECIMAL(18, 2) SUM,
    hot_score DECIMAL(18, 4) MAX
)
AGGREGATE KEY(stat_hour, sku_id, spu_code, sku_name, brand_name, season, wave_band, series, product_type)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE dws_hot_sku_rank_rt (
    stat_hour DATETIME,
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    hot_score DECIMAL(18, 4) MAX,
    rank_no INT MAX,
    reorder_suggest_flag TINYINT MAX
)
AGGREGATE KEY(stat_hour, brand_name, season, product_type, sku_id, spu_code, sku_name)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
```

## 五、Flink SQL：从 Paimon DWD 实时写入 Doris DWS

> 该部分在 Flink SQL 中执行。思路是：Paimon 作为 Source，Doris 作为实时聚合结果 Sink。
>
> 重要说明：Flink 的 `TUMBLE / HOP / CUMULATE` 窗口函数要求时间列必须是 `time attribute`，也就是除了字段类型为 `TIMESTAMP(3)` 之外，还必须定义 `WATERMARK`。
> 如果当前 DWD 表中的 `event_time` 只是普通时间字段，那么直接执行窗口聚合会报错：
>
> `The window function requires the timecol is a time attribute type, but is TIMESTAMP(3)`
>
> 因此，进入 DWS 之前，需要先为参与窗口聚合的 DWD 来源补齐水位定义。建议至少补齐以下 3 张表：
>
> - `dwd_inventory_flow_detail_rt`
> - `dwd_order_trade_detail_rt`
> - `dwd_hot_sku_event_detail_rt`

### 5.1 Flink Catalog 与 Doris Sink 定义

```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://192.168.63.128:8020/paimon/erp_dw'
);
USE CATALOG paimon_catalog;

-- Doris Sink：库存 ATP
CREATE TEMPORARY TABLE doris_dws_inventory_atp_rt (
    stat_minute TIMESTAMP(3),
    store_id STRING,
    warehouse_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    product_type STRING,
    available_qty DECIMAL(18, 2),
    locked_qty DECIMAL(18, 2),
    in_transit_qty DECIMAL(18, 2),
    stock_delta_qty DECIMAL(18, 2),
    atp_qty DECIMAL(18, 2)
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.63.128:7030',
    'table.identifier' = 'dws.dws_inventory_atp_rt',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.enable-2pc' = 'false'
);

-- Doris Sink：库存调拨建议
CREATE TEMPORARY TABLE doris_dws_inventory_dispatch_rt (
    stat_minute TIMESTAMP(3),
    store_id STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    product_type STRING,
    atp_qty DECIMAL(18, 2),
    order_qty DECIMAL(18, 2),
    order_amount DECIMAL(18, 2),
    shortage_risk_flag INT,
    dispatch_priority_score DECIMAL(18, 4)
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.63.128:7030',
    'table.identifier' = 'dws.dws_inventory_dispatch_rt',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.enable-2pc' = 'false'
);

-- Doris Sink：爆款特征
CREATE TEMPORARY TABLE doris_dws_hot_sku_feature_rt (
    stat_hour TIMESTAMP(3),
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    brand_name STRING,
    season STRING,
    wave_band STRING,
    series STRING,
    product_type STRING,
    sale_amount_1h DECIMAL(18, 2),
    order_cnt_1h BIGINT,
    view_cnt_1h BIGINT,
    click_cnt_1h BIGINT,
    add_cart_cnt_1h BIGINT,
    social_hot_1h DECIMAL(18, 2),
    hot_score DECIMAL(18, 4)
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.63.128:7030',
    'table.identifier' = 'dws.dws_hot_sku_feature_rt',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.enable-2pc' = 'false'
);

-- Doris Sink：爆款排名
CREATE TEMPORARY TABLE doris_dws_hot_sku_rank_rt (
    stat_hour TIMESTAMP(3),
    brand_name STRING,
    season STRING,
    product_type STRING,
    sku_id STRING,
    spu_code STRING,
    sku_name STRING,
    hot_score DECIMAL(18, 4),
    rank_no INT,
    reorder_suggest_flag INT
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.63.128:7030',
    'table.identifier' = 'dws.dws_hot_sku_rank_rt',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.enable-2pc' = 'false'
);
```

### 5.1.1 DWS 窗口来源的时间属性修正

> 推荐做法：在 DWD 层直接把 `event_time` 定义为带 `WATERMARK` 的事件时间列。
> 如果当前生产环境中的 DWD 表已经是“无水位版本”，可先新增一组 `_wm` 版本表，专门作为 DWS 窗口聚合输入。

```sql
-- 1. 库存流水 DWD（带水位版）
CREATE TABLE dwd.dwd_inventory_flow_detail_rt_wm (
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
    PRIMARY KEY (txn_id, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'ingest_time'
);

INSERT INTO dwd.dwd_inventory_flow_detail_rt_wm
SELECT *
FROM dwd.dwd_inventory_flow_detail_rt;

-- 2. 订单交易 DWD（带水位版）
CREATE TABLE dwd.dwd_order_trade_detail_rt_wm (
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
    'sequence.field' = 'ingest_time'
);

INSERT INTO dwd.dwd_order_trade_detail_rt_wm
SELECT *
FROM dwd.dwd_order_trade_detail_rt;

-- 3. 爆款事件归一化 DWD（带水位版）
CREATE TABLE dwd.dwd_hot_sku_event_detail_rt_wm (
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

INSERT INTO dwd.dwd_hot_sku_event_detail_rt_wm
SELECT *
FROM dwd.dwd_hot_sku_event_detail_rt;
```

### 5.1.2 Doris Sink 连通性排查

> 如果 Flink SQL Client 执行 `INSERT INTO doris_*` 时出现：
>
> `java.io.IOException: Failed to get response from Doris`
>
> 优先排查以下 4 项：
>
> - `fenodes` 是否配置为 FE 的 HTTP 端口。Flink Doris Connector 写入通常使用 `8030`，不是 MySQL 查询端口 `9030`
> - `table.identifier` 对应的库表是否已在 Doris 内部库中提前创建
> - `username / password` 是否具备目标库表写权限
> - Flink TaskManager 所在机器到 Doris FE `8030` 端口是否网络可达
>
> 当前文档中的 Doris Sink 已统一按 `192.168.63.128:8030` 编写。

### 5.2 场景一：库存 ATP 汇总

```sql
INSERT INTO doris_dws_inventory_atp_rt
SELECT
    window_start AS stat_minute,
    store_id,
    warehouse_id,
    sku_id,
    MAX(spu_code) AS spu_code,
    MAX(sku_name) AS sku_name,
    MAX(brand_name) AS brand_name,
    MAX(season) AS season,
    MAX(product_type) AS product_type,
    SUM(CASE WHEN stock_status = 'AVAILABLE' THEN qty ELSE 0 END) AS available_qty,
    ABS(SUM(CASE WHEN stock_status = 'LOCKED' THEN qty ELSE 0 END)) AS locked_qty,
    SUM(CASE WHEN stock_status = 'IN_TRANSIT' THEN qty ELSE 0 END) AS in_transit_qty,
    SUM(qty) AS stock_delta_qty,
    SUM(CASE WHEN stock_status = 'AVAILABLE' THEN qty ELSE 0 END)
      + SUM(CASE WHEN stock_status = 'IN_TRANSIT' THEN qty ELSE 0 END)
      - ABS(SUM(CASE WHEN stock_status = 'LOCKED' THEN qty ELSE 0 END)) AS atp_qty
FROM TABLE(
    TUMBLE(TABLE dwd.dwd_inventory_flow_detail_rt_wm, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, store_id, warehouse_id, sku_id;
```

### 5.3 场景一：库存调拨建议汇总

> 说明：Flink SQL Client 在部分版本下，执行“窗口聚合临时视图 + 再次 Join”的多语句写法时，可能触发 Planner 冲突报错：
>
> `Relational expression ... belongs to a different planner than is currently being used`
>
> 建议直接使用单条 `INSERT ... WITH ... SELECT ...`，避免 `CREATE TEMPORARY VIEW` 带来的计划器上下文切换问题。

```sql
INSERT INTO doris_dws_inventory_dispatch_rt
WITH v_inventory_atp_1m AS (
    SELECT
        window_start AS stat_minute,
        store_id,
        sku_id,
        MAX(spu_code) AS spu_code,
        MAX(sku_name) AS sku_name,
        MAX(brand_name) AS brand_name,
        MAX(season) AS season,
        MAX(product_type) AS product_type,
        SUM(CASE WHEN stock_status = 'AVAILABLE' THEN qty ELSE 0 END)
          + SUM(CASE WHEN stock_status = 'IN_TRANSIT' THEN qty ELSE 0 END)
          - ABS(SUM(CASE WHEN stock_status = 'LOCKED' THEN qty ELSE 0 END)) AS atp_qty
    FROM TABLE(
        TUMBLE(TABLE dwd.dwd_inventory_flow_detail_rt_wm, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, store_id, sku_id
),
v_order_demand_1m AS (
    SELECT
        window_start AS stat_minute,
        store_id,
        sku_id,
        SUM(qty) AS order_qty,
        SUM(sale_amount) AS order_amount
    FROM TABLE(
        TUMBLE(TABLE dwd.dwd_order_trade_detail_rt_wm, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, store_id, sku_id
)
SELECT
    i.stat_minute,
    i.store_id,
    i.sku_id,
    i.spu_code,
    i.sku_name,
    i.brand_name,
    i.season,
    i.product_type,
    i.atp_qty,
    COALESCE(o.order_qty, CAST(0 AS DECIMAL(18, 2))) AS order_qty,
    COALESCE(o.order_amount, CAST(0 AS DECIMAL(18, 2))) AS order_amount,
    CASE
        WHEN i.atp_qty < COALESCE(o.order_qty, CAST(0 AS DECIMAL(18, 2))) THEN 1
        ELSE 0
    END AS shortage_risk_flag,
    CASE
        WHEN i.atp_qty <= 0 THEN CAST(100 AS DECIMAL(18, 4))
        WHEN o.order_qty IS NULL THEN CAST(0 AS DECIMAL(18, 4))
        ELSE CAST(o.order_qty / NULLIF(i.atp_qty, 0) AS DECIMAL(18, 4))
    END AS dispatch_priority_score
FROM v_inventory_atp_1m i
LEFT JOIN v_order_demand_1m o
ON i.stat_minute = o.stat_minute
AND i.store_id = o.store_id
AND i.sku_id = o.sku_id;
```

### 5.4 场景二：爆款特征汇总

```sql
CREATE TEMPORARY VIEW v_hot_sku_feature_1h AS
SELECT
    window_start AS stat_hour,
    sku_id,
    MAX(spu_code) AS spu_code,
    MAX(sku_name) AS sku_name,
    MAX(brand_name) AS brand_name,
    MAX(season) AS season,
    MAX(wave_band) AS wave_band,
    MAX(series) AS series,
    MAX(product_type) AS product_type,
    SUM(CASE WHEN event_source = 'ORDER' THEN metric_value ELSE 0 END) AS sale_amount_1h,
    SUM(CASE WHEN event_source = 'ORDER' THEN 1 ELSE 0 END) AS order_cnt_1h,
    SUM(CASE WHEN metric_type = 'VIEW' THEN 1 ELSE 0 END) AS view_cnt_1h,
    SUM(CASE WHEN metric_type = 'CLICK' THEN 1 ELSE 0 END) AS click_cnt_1h,
    SUM(CASE WHEN metric_type = 'ADD_CART' THEN 1 ELSE 0 END) AS add_cart_cnt_1h,
    SUM(CASE WHEN event_source = 'SOCIAL' THEN metric_value ELSE 0 END) AS social_hot_1h,
    CAST(
        SUM(CASE WHEN event_source = 'ORDER' THEN metric_value ELSE 0 END) * 0.45 +
        SUM(CASE WHEN metric_type = 'CLICK' THEN 1 ELSE 0 END) * 0.10 +
        SUM(CASE WHEN metric_type = 'ADD_CART' THEN 1 ELSE 0 END) * 0.20 +
        SUM(CASE WHEN event_source = 'SOCIAL' THEN metric_value ELSE 0 END) * 0.25
        AS DECIMAL(18, 4)
    ) AS hot_score
FROM TABLE(
    TUMBLE(TABLE dwd.dwd_hot_sku_event_detail_rt_wm, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
)
GROUP BY window_start, sku_id;

INSERT INTO doris_dws_hot_sku_feature_rt
SELECT *
FROM v_hot_sku_feature_1h;
```

### 5.5 场景二：爆款排行汇总

> 说明：Flink SQL 对严格 TopN 排名的实现较复杂，生产可先在 Flink 侧输出“排行候选结果”，再由 Doris 查询层使用 `ROW_NUMBER()` 或物化视图实现最终榜单。

```sql
INSERT INTO doris_dws_hot_sku_rank_rt
SELECT
    stat_hour,
    brand_name,
    season,
    product_type,
    sku_id,
    spu_code,
    sku_name,
    hot_score,
    CAST(0 AS INT) AS rank_no,
    CASE
        WHEN hot_score >= 500 THEN 1
        ELSE 0
    END AS reorder_suggest_flag
FROM v_hot_sku_feature_1h;
```

## 六、Doris 物化视图建议

> 该部分在 Doris 中执行，用于对 DWS 结果进一步加速。

```sql
-- 库存驾驶舱分钟聚合
CREATE MATERIALIZED VIEW mv_inventory_dispatch_rt
BUILD IMMEDIATE
REFRESH AUTO
ON SCHEDULE EVERY 1 MINUTE
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    stat_minute,
    store_id,
    sku_id,
    atp_qty,
    order_qty,
    shortage_risk_flag,
    dispatch_priority_score
FROM dws.dws_inventory_dispatch_rt;

-- 爆款小时榜
CREATE MATERIALIZED VIEW mv_hot_sku_feature_1h
BUILD IMMEDIATE
REFRESH AUTO
ON SCHEDULE EVERY 1 MINUTE
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    stat_hour,
    sku_id,
    spu_code,
    sku_name,
    hot_score,
    sale_amount_1h,
    order_cnt_1h,
    social_hot_1h
FROM dws.dws_hot_sku_feature_rt;
```

## 七、校验建议 SQL

```sql
-- 校验 ATP 是否出现异常负值
SELECT *
FROM dws.dws_inventory_atp_rt
WHERE atp_qty < -100;

-- 校验调拨建议高优先级商品
SELECT *
FROM dws.dws_inventory_dispatch_rt
WHERE shortage_risk_flag = 1
ORDER BY dispatch_priority_score DESC;

-- 校验爆款特征是否正常产出
SELECT stat_hour, COUNT(*) AS sku_cnt
FROM dws.dws_hot_sku_feature_rt
GROUP BY stat_hour
ORDER BY stat_hour DESC;

-- 校验爆款建议标签
SELECT *
FROM dws.dws_hot_sku_rank_rt
WHERE reorder_suggest_flag = 1
ORDER BY hot_score DESC;
```

## 八、落地建议

- DWS 结果存储在 Doris 内部表，承担服务层职责
- Paimon 继续承担 DIM / DWD 明细沉淀职责
- Doris 外部表用于明细排查和灵活分析，DWS 内部表用于高频查询
- 后续可继续在 Doris 内部 DWS 表之上建设 ADS 主题表和异步物化视图
