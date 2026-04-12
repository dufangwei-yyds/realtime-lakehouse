# 实时湖仓ADS建模实现方案

> 本文档为服装行业实时湖仓 ADS 层生产版建模实现方案。  
> 当前定位：从零开始建设 ADS 层，直接面向业务看板、专题分析与数据服务接口。  
> 上游依赖：已完成并验证的 DIM / ODS / DWD / DWS 四层实现。  
> 技术边界：本文仅描述 ADS 如何为 Superset 和数据接口提供数据支撑，不展开 Superset 部署过程。

---

## 一、ADS 层建设说明

ADS（Application Data Service）层是实时湖仓中最贴近业务消费的一层，核心目标是把上游 DWS 能力组织为可直接被业务使用的专题数据集、接口结果集和经营驾驶舱数据模型。

在当前服装行业实时湖仓建设中，ADS 层主要服务两类消费方式：

- 面向 Superset 的 BI 看板与专题分析
- 面向业务系统的数据服务接口与结果推送

结合一期建设范围，ADS 层优先围绕以下两大核心场景展开：

1. 全渠道实时库存可视与智能调拨
2. 爆款趋势识别与快速追单

---

## 二、ADS 层核心依赖对象

### 2.1 上游 DWS 依赖

| 场景           | DWS 输入表                  | 作用                                                 |
| -------------- | --------------------------- | ---------------------------------------------------- |
| 库存可视与调拨 | `dws_inventory_atp_rt`      | 提供分钟级 ATP、在途、锁定、库存变动                 |
| 库存可视与调拨 | `dws_inventory_dispatch_rt` | 提供缺货风险、订单需求、调拨优先级                   |
| 爆款趋势识别   | `dws_hot_sku_feature_rt`    | 提供小时级销量、浏览、点击、加购、社媒热度、爆款分值 |
| 爆款趋势识别   | `dws_hot_sku_rank_rt`       | 提供爆款候选榜单与追单建议标签                       |

### 2.2 必要的 DWD / DIM 依赖

| 层级 | 依赖对象                       | 作用                                     |
| ---- | ------------------------------ | ---------------------------------------- |
| DIM  | `dim_sku_wide`                 | 商品基础维度补充、中文展示字段、分类钻取 |
| DWD  | `dwd_inventory_flow_detail_rt` | 异常明细排查与库存波动解释               |
| DWD  | `dwd_order_trade_detail_rt`    | 调拨需求与订单明细钻取                   |
| DWD  | `dwd_hot_sku_event_detail_rt`  | 热度归因、趋势解释、事件回溯             |

### 2.3 消费端依赖

- Doris：承担 ADS 主题表存储、查询加速、异步物化视图
- Superset：消费 Doris ADS 主题表，构建看板与分析图表
- 数据服务接口：消费 Doris ADS 结果表，输出分页、筛选、排序后的结果集

---

## 三、ADS 层建设目标

围绕“可用、可看、可解释、可服务”四个目标建设 ADS：

1. 可用：业务方可以直接查询或消费，不再要求其理解 DWD / DWS 细节
2. 可看：支持 Superset 秒级出图、钻取、筛选、排行和趋势展示
3. 可解释：预警、推荐、排行结果必须附带原因、规则、更新时间
4. 可服务：结果集可以直接作为接口返回，减少业务系统二次加工

### 设计原则

- 面向场景：按业务主题建模，不按技术组件建模
- 面向消费：字段设计围绕看板、接口、告警推送展开
- 指标承接：核心口径承接 DWS，不在 ADS 重新发明指标
- 查询优先：优先保证秒级查询与稳定分页
- 可解释性：推荐类结果必须带规则标签与触发原因
- 一表一责：同一 ADS 表只服务一类稳定消费场景

---

## 四、ADS 目标主题与表设计

### 4.1 主题划分

| 场景           | ADS 主题       | 面向对象            | 主要用途                            |
| -------------- | -------------- | ------------------- | ----------------------------------- |
| 库存可视与调拨 | 库存驾驶舱主题 | Superset 看板       | 实时库存、ATP、缺货风险、店仓健康度 |
| 库存可视与调拨 | 调拨服务主题   | 数据接口 / 运营动作 | 输出调拨建议、优先级、触发原因      |
| 爆款趋势识别   | 爆款看板主题   | Superset 看板       | 爆款榜单、热度走势、趋势分析        |
| 爆款趋势识别   | 追单服务主题   | 数据接口 / 企划运营 | 输出高潜 SKU、追单建议、热度归因    |

### 4.2 ADS 目标表

| ADS 表                             | 类型       | 粒度              | 主要服务对象               |
| ---------------------------------- | ---------- | ----------------- | -------------------------- |
| `ads_inventory_dispatch_dashboard` | 看板型 ADS | 分钟 + 店仓 + SKU | 库存看板、驾驶舱、经营专题 |
| `ads_inventory_dispatch_api`       | 接口型 ADS | 分钟 + 店仓 + SKU | 调拨建议接口、预警推送     |
| `ads_hot_sku_dashboard`            | 看板型 ADS | 小时 + SKU        | 爆款趋势看板、商品分析     |
| `ads_hot_sku_reorder_api`          | 接口型 ADS | 小时 + SKU        | 追单建议接口、商品企划动作 |

---

## 五、DWS 到 ADS 的输入输出映射

### 5.1 库存主题映射

| DWS 输入                                            | ADS 输出                                   | 映射说明         |
| --------------------------------------------------- | ------------------------------------------ | ---------------- |
| `dws_inventory_atp_rt.atp_qty`                      | `ads_inventory_dispatch_dashboard.atp_qty` | 直接承接 ATP     |
| `dws_inventory_atp_rt.available_qty`                | `available_qty`                            | 直接展示可用库存 |
| `dws_inventory_atp_rt.locked_qty`                   | `locked_qty`                               | 直接展示锁定库存 |
| `dws_inventory_atp_rt.in_transit_qty`               | `in_transit_qty`                           | 直接展示在途库存 |
| `dws_inventory_dispatch_rt.order_qty`               | `demand_qty`                               | 订单需求量       |
| `dws_inventory_dispatch_rt.shortage_risk_flag`      | `shortage_risk_flag`                       | 缺货风险标识     |
| `dws_inventory_dispatch_rt.dispatch_priority_score` | `dispatch_priority_score`                  | 调拨优先级分值   |

### 5.2 爆款主题映射

| DWS 输入                                   | ADS 输出               | 映射说明     |
| ------------------------------------------ | ---------------------- | ------------ |
| `dws_hot_sku_feature_rt.sale_amount_1h`    | `sale_amount_1h`       | 小时销售额   |
| `dws_hot_sku_feature_rt.order_cnt_1h`      | `order_cnt_1h`         | 小时订单数   |
| `dws_hot_sku_feature_rt.view_cnt_1h`       | `view_cnt_1h`          | 小时浏览数   |
| `dws_hot_sku_feature_rt.click_cnt_1h`      | `click_cnt_1h`         | 小时点击数   |
| `dws_hot_sku_feature_rt.add_cart_cnt_1h`   | `add_cart_cnt_1h`      | 小时加购数   |
| `dws_hot_sku_feature_rt.social_hot_1h`     | `social_hot_1h`        | 小时社媒热度 |
| `dws_hot_sku_feature_rt.hot_score`         | `hot_score`            | 爆款综合指数 |
| `dws_hot_sku_rank_rt.reorder_suggest_flag` | `reorder_suggest_flag` | 追单建议标识 |

---

## 六、Doris 内部 ADS 目标表实现

> 本节在 Doris 中执行。ADS 层统一落 Doris 内部库，直接服务 BI 与数据接口。

```sql
-- Doris SQL
SWITCH internal;
CREATE DATABASE IF NOT EXISTS ads;
USE ads;

CREATE TABLE ads_inventory_dispatch_dashboard (
    stat_minute DATETIME,
    store_id VARCHAR(32),
    warehouse_id VARCHAR(32),
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    available_qty DECIMAL(18, 2),
    locked_qty DECIMAL(18, 2),
    in_transit_qty DECIMAL(18, 2),
    atp_qty DECIMAL(18, 2),
    demand_qty DECIMAL(18, 2),
    shortage_risk_flag TINYINT,
    dispatch_priority_score DECIMAL(18, 4),
    inventory_health_level VARCHAR(16),
    alert_reason VARCHAR(256),
    update_time DATETIME
)
UNIQUE KEY(stat_minute, store_id, warehouse_id, sku_id)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE ads_inventory_dispatch_api (
    stat_minute DATETIME,
    store_id VARCHAR(32),
    warehouse_id VARCHAR(32),
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    atp_qty DECIMAL(18, 2),
    demand_qty DECIMAL(18, 2),
    dispatch_priority_score DECIMAL(18, 4),
    shortage_risk_flag TINYINT,
    suggest_action VARCHAR(64),
    trigger_reason VARCHAR(256),
    update_time DATETIME
)
UNIQUE KEY(stat_minute, store_id, warehouse_id, sku_id)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE ads_hot_sku_dashboard (
    stat_hour DATETIME,
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    wave_band VARCHAR(64),
    series VARCHAR(64),
    product_type VARCHAR(64),
    sale_amount_1h DECIMAL(18, 2),
    order_cnt_1h BIGINT,
    view_cnt_1h BIGINT,
    click_cnt_1h BIGINT,
    add_cart_cnt_1h BIGINT,
    social_hot_1h DECIMAL(18, 2),
    hot_score DECIMAL(18, 4),
    hot_level VARCHAR(16),
    trend_label VARCHAR(32),
    rank_no INT,
    update_time DATETIME
)
UNIQUE KEY(stat_hour, sku_id)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE ads_hot_sku_reorder_api (
    stat_hour DATETIME,
    sku_id VARCHAR(64),
    spu_code VARCHAR(64),
    sku_name VARCHAR(128),
    brand_name VARCHAR(64),
    season VARCHAR(32),
    product_type VARCHAR(64),
    hot_score DECIMAL(18, 4),
    rank_no INT,
    reorder_suggest_flag TINYINT,
    reorder_level VARCHAR(16),
    trigger_reason VARCHAR(256),
    stock_support_flag TINYINT,
    update_time DATETIME
)
UNIQUE KEY(stat_hour, sku_id)
DISTRIBUTED BY HASH(sku_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
```

---

## 七、ADS 主题 SQL 实现

> 推荐实现方式：DWS 保持实时聚合，ADS 在 Doris 中通过定时 `INSERT OVERWRITE`、异步物化视图或轻量刷新作业完成结果集收口。  
> 一期先以 Doris SQL 为主，优先保证结果稳定、可解释、可服务。

### 7.1 库存驾驶舱主题

```sql
INSERT OVERWRITE TABLE ads.ads_inventory_dispatch_dashboard
SELECT
    a.stat_minute,
    a.store_id,
    a.warehouse_id,
    a.sku_id,
    a.spu_code,
    a.sku_name,
    a.brand_name,
    a.season,
    a.product_type,
    a.available_qty,
    a.locked_qty,
    a.in_transit_qty,
    a.atp_qty,
    COALESCE(d.order_qty, 0) AS demand_qty,
    COALESCE(d.shortage_risk_flag, 0) AS shortage_risk_flag,
    COALESCE(d.dispatch_priority_score, 0) AS dispatch_priority_score,
    CASE
        WHEN a.atp_qty <= 0 THEN 'RED'
        WHEN a.atp_qty < COALESCE(d.order_qty, 0) THEN 'ORANGE'
        ELSE 'GREEN'
    END AS inventory_health_level,
    CASE
        WHEN a.atp_qty <= 0 THEN 'ATP小于等于0'
        WHEN a.atp_qty < COALESCE(d.order_qty, 0) THEN 'ATP低于需求量'
        WHEN COALESCE(d.shortage_risk_flag, 0) = 1 THEN '存在缺货风险'
        ELSE '库存健康'
    END AS alert_reason,
    NOW() AS update_time
FROM dws.dws_inventory_atp_rt a
LEFT JOIN dws.dws_inventory_dispatch_rt d
ON a.stat_minute = d.stat_minute
AND a.store_id = d.store_id
AND a.sku_id = d.sku_id;
```

### 7.2 调拨服务主题

```sql
INSERT OVERWRITE TABLE ads.ads_inventory_dispatch_api
SELECT
    stat_minute,
    store_id,
    warehouse_id,
    sku_id,
    spu_code,
    sku_name,
    brand_name,
    season,
    product_type,
    atp_qty,
    demand_qty,
    dispatch_priority_score,
    shortage_risk_flag,
    CASE
        WHEN shortage_risk_flag = 1 AND dispatch_priority_score >= 1 THEN 'URGENT_DISPATCH'
        WHEN shortage_risk_flag = 1 THEN 'NORMAL_DISPATCH'
        ELSE 'NO_ACTION'
    END AS suggest_action,
    CASE
        WHEN atp_qty <= 0 THEN '当前可承诺库存不足'
        WHEN atp_qty < demand_qty THEN '库存小于订单需求'
        ELSE '库存满足需求'
    END AS trigger_reason,
    update_time
FROM ads.ads_inventory_dispatch_dashboard;
```

### 7.3 爆款看板主题

```sql
INSERT OVERWRITE TABLE ads.ads_hot_sku_dashboard
SELECT
    f.stat_hour,
    f.sku_id,
    f.spu_code,
    f.sku_name,
    f.brand_name,
    f.season,
    f.wave_band,
    f.series,
    f.product_type,
    f.sale_amount_1h,
    f.order_cnt_1h,
    f.view_cnt_1h,
    f.click_cnt_1h,
    f.add_cart_cnt_1h,
    f.social_hot_1h,
    f.hot_score,
    CASE
        WHEN f.hot_score >= 800 THEN 'S'
        WHEN f.hot_score >= 500 THEN 'A'
        WHEN f.hot_score >= 300 THEN 'B'
        ELSE 'C'
    END AS hot_level,
    CASE
        WHEN f.order_cnt_1h >= 20 AND f.social_hot_1h >= 100 THEN '销量社媒双高'
        WHEN f.order_cnt_1h >= 20 THEN '销量快速增长'
        WHEN f.social_hot_1h >= 100 THEN '社媒热度升高'
        ELSE '常规波动'
    END AS trend_label,
    ROW_NUMBER() OVER (
        PARTITION BY f.stat_hour, f.brand_name, f.season, f.product_type
        ORDER BY f.hot_score DESC
    ) AS rank_no,
    NOW() AS update_time
FROM dws.dws_hot_sku_feature_rt f;
```

### 7.4 追单服务主题

```sql
INSERT OVERWRITE TABLE ads.ads_hot_sku_reorder_api
SELECT
    d.stat_hour,
    d.sku_id,
    d.spu_code,
    d.sku_name,
    d.brand_name,
    d.season,
    d.product_type,
    d.hot_score,
    d.rank_no,
    CASE
        WHEN d.hot_score >= 500 THEN 1
        ELSE 0
    END AS reorder_suggest_flag,
    CASE
        WHEN d.hot_score >= 800 THEN 'HIGH'
        WHEN d.hot_score >= 500 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS reorder_level,
    CONCAT(
        '热度等级=', d.hot_level,
        '; 趋势标签=', d.trend_label,
        '; 排名=', CAST(d.rank_no AS STRING)
    ) AS trigger_reason,
    CASE
        WHEN d.rank_no <= 20 THEN 1
        ELSE 0
    END AS stock_support_flag,
    d.update_time
FROM ads.ads_hot_sku_dashboard d;
```

---

## 八、Superset 消费口径设计

> Superset 已完成搭建，本节仅定义 ADS 如何为看板提供数据集与查询口径，不涉及部署安装。

### 8.1 Superset 数据集建议

| 数据集                                 | 来源表                                 | 用途         |
| -------------------------------------- | -------------------------------------- | ------------ |
| `dataset_inventory_dispatch_dashboard` | `ads.ads_inventory_dispatch_dashboard` | 库存驾驶舱   |
| `dataset_hot_sku_dashboard`            | `ads.ads_hot_sku_dashboard`            | 爆款趋势看板 |

### 8.2 库存驾驶舱建议图表

| 图表               | 来源字段                                                | 说明                 |
| ------------------ | ------------------------------------------------------- | -------------------- |
| 店仓 ATP 热力图    | `store_id, warehouse_id, atp_qty`                       | 看各店仓库存健康情况 |
| 缺货风险排行榜     | `sku_name, shortage_risk_flag, dispatch_priority_score` | 排查高风险商品       |
| 在途与锁定库存对比 | `in_transit_qty, locked_qty`                            | 识别占用和履约压力   |
| 库存健康分层       | `inventory_health_level`                                | 识别绿/橙/红健康分布 |

### 8.3 爆款趋势看板建议图表

| 图表            | 来源字段                        | 说明                 |
| --------------- | ------------------------------- | -------------------- |
| 爆款 TopN 榜单  | `sku_name, hot_score, rank_no`  | 看当前热销和高潜商品 |
| 热度趋势折线图  | `stat_hour, hot_score`          | 观察小时级趋势       |
| 销量/社媒双轴图 | `sale_amount_1h, social_hot_1h` | 看成交与热度联动     |
| 品类爆款分布    | `product_type, hot_level`       | 看不同品类热度层次   |

### 8.4 看板查询口径约束

- Superset 优先直连 ADS 主题表，不直接扫 DWD/DWS 明细大表
- 默认时间粒度：
  - 库存场景：分钟级
  - 爆款场景：小时级
- 默认排序口径：
  - 库存场景：按 `dispatch_priority_score` 降序
  - 爆款场景：按 `hot_score` 或 `rank_no` 排序
- 钻取路径：
  - ADS -> DWS 汇总 -> DWD 明细

---

## 九、数据服务接口输出模型

> 说明：以下 Java 代码仅作为接口实现参考，假设应用层通过 JDBC / MyBatis / JPA 访问 Doris ADS 结果表，对外提供 REST API。

### 9.0 统一返回与分页模型示例

```java
public record ApiResponse<T>(
        String code,
        String message,
        T data
) {
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>("0", "success", data);
    }
}

public record PageRequest(
        Integer pageNo,
        Integer pageSize
) {
    public int offset() {
        int safePageNo = (pageNo == null || pageNo < 1) ? 1 : pageNo;
        int safePageSize = (pageSize == null || pageSize < 1) ? 20 : pageSize;
        return (safePageNo - 1) * safePageSize;
    }

    public int limit() {
        return (pageSize == null || pageSize < 1) ? 20 : pageSize;
    }
}

public record PageResult<T>(
        long total,
        Integer pageNo,
        Integer pageSize,
        List<T> records
) {}
```

### 9.1 调拨建议接口

#### 返回对象

| 字段                      | 来源                                     | 说明       |
| ------------------------- | ---------------------------------------- | ---------- |
| `stat_minute`             | `ads_inventory_dispatch_api.stat_minute` | 统计时间   |
| `store_id`                | `store_id`                               | 门店       |
| `warehouse_id`            | `warehouse_id`                           | 仓库       |
| `sku_id`                  | `sku_id`                                 | SKU        |
| `sku_name`                | `sku_name`                               | 商品名称   |
| `atp_qty`                 | `atp_qty`                                | 可承诺库存 |
| `demand_qty`              | `demand_qty`                             | 当前需求量 |
| `dispatch_priority_score` | `dispatch_priority_score`                | 调拨优先级 |
| `suggest_action`          | `suggest_action`                         | 建议动作   |
| `trigger_reason`          | `trigger_reason`                         | 触发原因   |
| `update_time`             | `update_time`                            | 更新时间   |

#### 查询示例

```sql
SELECT *
FROM ads.ads_inventory_dispatch_api
WHERE stat_minute >= NOW() - INTERVAL 30 MINUTE
  AND shortage_risk_flag = 1
ORDER BY dispatch_priority_score DESC
LIMIT 100;
```

#### Java API 服务示例

```java
public record InventoryDispatchSuggestionDTO(
        LocalDateTime statMinute,
        String storeId,
        String warehouseId,
        String skuId,
        String skuName,
        BigDecimal atpQty,
        BigDecimal demandQty,
        BigDecimal dispatchPriorityScore,
        String suggestAction,
        String triggerReason,
        LocalDateTime updateTime
) {}

@RestController
@RequestMapping("/api/ads/inventory")
public class InventoryDispatchController {

    private final JdbcTemplate jdbcTemplate;

    public InventoryDispatchController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/dispatch-suggestions")
    public ApiResponse<PageResult<InventoryDispatchSuggestionDTO>> queryDispatchSuggestions(
            @RequestParam(required = false) String storeId,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "20") Integer pageSize) {

        PageRequest pageRequest = new PageRequest(pageNo, pageSize);

        String sql = """
            SELECT
                stat_minute,
                store_id,
                warehouse_id,
                sku_id,
                sku_name,
                atp_qty,
                demand_qty,
                dispatch_priority_score,
                suggest_action,
                trigger_reason,
                update_time
            FROM ads.ads_inventory_dispatch_api
            WHERE shortage_risk_flag = 1
              AND (? IS NULL OR store_id = ?)
            ORDER BY dispatch_priority_score DESC, stat_minute DESC
            LIMIT ? OFFSET ?
            """;

        String countSql = """
            SELECT COUNT(1)
            FROM ads.ads_inventory_dispatch_api
            WHERE shortage_risk_flag = 1
              AND (? IS NULL OR store_id = ?)
            """;

        Long total = jdbcTemplate.queryForObject(countSql, Long.class, storeId, storeId);

        List<InventoryDispatchSuggestionDTO> records = jdbcTemplate.query(
                sql,
                new Object[]{storeId, storeId, pageRequest.limit(), pageRequest.offset()},
                (rs, rowNum) -> new InventoryDispatchSuggestionDTO(
                        rs.getTimestamp("stat_minute").toLocalDateTime(),
                        rs.getString("store_id"),
                        rs.getString("warehouse_id"),
                        rs.getString("sku_id"),
                        rs.getString("sku_name"),
                        rs.getBigDecimal("atp_qty"),
                        rs.getBigDecimal("demand_qty"),
                        rs.getBigDecimal("dispatch_priority_score"),
                        rs.getString("suggest_action"),
                        rs.getString("trigger_reason"),
                        rs.getTimestamp("update_time").toLocalDateTime()
                )
        );

        return ApiResponse.success(
                new PageResult<>(
                        total == null ? 0L : total,
                        pageNo,
                        pageSize,
                        records
                )
        );
    }
}
```

### 9.2 追单建议接口

#### 返回对象

| 字段                   | 来源                                | 说明             |
| ---------------------- | ----------------------------------- | ---------------- |
| `stat_hour`            | `ads_hot_sku_reorder_api.stat_hour` | 统计小时         |
| `sku_id`               | `sku_id`                            | SKU              |
| `sku_name`             | `sku_name`                          | 商品名称         |
| `hot_score`            | `hot_score`                         | 爆款分值         |
| `rank_no`              | `rank_no`                           | 排名             |
| `reorder_suggest_flag` | `reorder_suggest_flag`              | 是否建议追单     |
| `reorder_level`        | `reorder_level`                     | 建议等级         |
| `trigger_reason`       | `trigger_reason`                    | 触发原因         |
| `stock_support_flag`   | `stock_support_flag`                | 是否具备库存支撑 |
| `update_time`          | `update_time`                       | 更新时间         |

#### 查询示例

```sql
SELECT *
FROM ads.ads_hot_sku_reorder_api
WHERE stat_hour >= NOW() - INTERVAL 6 HOUR
  AND reorder_suggest_flag = 1
ORDER BY hot_score DESC, rank_no ASC
LIMIT 100;
```

#### Java API 服务示例

```java
public record HotSkuReorderDTO(
        LocalDateTime statHour,
        String skuId,
        String skuName,
        String brandName,
        BigDecimal hotScore,
        Integer rankNo,
        Integer reorderSuggestFlag,
        String reorderLevel,
        String triggerReason,
        Integer stockSupportFlag,
        LocalDateTime updateTime
) {}

@RestController
@RequestMapping("/api/ads/hot-sku")
public class HotSkuReorderController {

    private final JdbcTemplate jdbcTemplate;

    public HotSkuReorderController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/reorder-suggestions")
    public ApiResponse<PageResult<HotSkuReorderDTO>> queryReorderSuggestions(
            @RequestParam(required = false) String brandName,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "20") Integer pageSize) {

        PageRequest pageRequest = new PageRequest(pageNo, pageSize);

        String sql = """
            SELECT
                stat_hour,
                sku_id,
                sku_name,
                brand_name,
                hot_score,
                rank_no,
                reorder_suggest_flag,
                reorder_level,
                trigger_reason,
                stock_support_flag,
                update_time
            FROM ads.ads_hot_sku_reorder_api
            WHERE reorder_suggest_flag = 1
              AND (? IS NULL OR brand_name = ?)
            ORDER BY hot_score DESC, rank_no ASC
            LIMIT ? OFFSET ?
            """;

        String countSql = """
            SELECT COUNT(1)
            FROM ads.ads_hot_sku_reorder_api
            WHERE reorder_suggest_flag = 1
              AND (? IS NULL OR brand_name = ?)
            """;

        Long total = jdbcTemplate.queryForObject(countSql, Long.class, brandName, brandName);

        List<HotSkuReorderDTO> records = jdbcTemplate.query(
                sql,
                new Object[]{brandName, brandName, pageRequest.limit(), pageRequest.offset()},
                (rs, rowNum) -> new HotSkuReorderDTO(
                        rs.getTimestamp("stat_hour").toLocalDateTime(),
                        rs.getString("sku_id"),
                        rs.getString("sku_name"),
                        rs.getString("brand_name"),
                        rs.getBigDecimal("hot_score"),
                        rs.getInt("rank_no"),
                        rs.getInt("reorder_suggest_flag"),
                        rs.getString("reorder_level"),
                        rs.getString("trigger_reason"),
                        rs.getInt("stock_support_flag"),
                        rs.getTimestamp("update_time").toLocalDateTime()
                )
        );

        return ApiResponse.success(
                new PageResult<>(
                        total == null ? 0L : total,
                        pageNo,
                        pageSize,
                        records
                )
        );
    }
}
```

---

## 十、接口设计规范建议

### 10.1 URL 设计建议

- 统一采用资源化路径风格，避免动词过多嵌套
- 建议统一前缀：`/api/ads/{domain}/{resource}`
- 示例：
  - `/api/ads/inventory/dispatch-suggestions`
  - `/api/ads/hot-sku/reorder-suggestions`

### 10.2 查询参数设计建议

- 公共分页参数统一为：
  - `pageNo`
  - `pageSize`
- 公共过滤参数建议按业务域分组，不使用无语义缩写
- 时间筛选统一明确字段语义：
  - 库存场景使用 `startMinute / endMinute`
  - 爆款场景使用 `startHour / endHour`
- 推荐保留的高频筛选参数：
  - 库存接口：`storeId`、`warehouseId`、`brandName`、`season`、`productType`
  - 爆款接口：`brandName`、`season`、`productType`、`hotLevel`

### 10.3 排序与分页规范

- 不允许前端直接传任意 SQL 排序字段
- 建议后端维护排序字段白名单：
  - 库存接口：`dispatchPriorityScore`、`atpQty`、`updateTime`
  - 爆款接口：`hotScore`、`rankNo`、`updateTime`
- 默认排序规则：
  - 调拨建议：`dispatchPriorityScore DESC, statMinute DESC`
  - 追单建议：`hotScore DESC, rankNo ASC`
- 默认分页大小建议 `20`
- 单次最大分页大小建议不超过 `200`

### 10.4 返回结构规范

- 统一使用：
  - `code`
  - `message`
  - `data`
- 分页结果统一包含：
  - `total`
  - `pageNo`
  - `pageSize`
  - `records`
- 推荐类接口必须补充：
  - `triggerReason`
  - `updateTime`
- 排行类接口必须补充：
  - `rankNo`
  - `hotScore` 或业务排序分值

### 10.5 错误码约定建议

| 错误码 | 含义           | 适用场景                   |
| ------ | -------------- | -------------------------- |
| `0`    | 成功           | 正常返回                   |
| `4001` | 参数非法       | 分页、时间、排序字段不合法 |
| `4002` | 排序字段不支持 | 非白名单字段               |
| `4003` | 时间范围非法   | 开始时间大于结束时间       |
| `5001` | 数据查询失败   | Doris 查询异常             |
| `5002` | 服务超时       | 接口超时或下游超时         |

### 10.6 缓存与限流建议

- 高频驾驶舱接口建议增加短 TTL 缓存，如 `10-30` 秒
- 爆款榜单接口建议支持按 `statHour` 维度缓存
- 对领导大屏、首页看板类接口建议增加网关限流
- 当 ADS 已有异步物化视图时，优先命中 MV 后再做接口缓存

### 10.7 实施建议

- BI 查询与接口查询不要共用完全相同的 SQL 模板
- 接口服务优先访问接口型 ADS 表，不直接复用看板型 ADS 表
- 后续如接入网关或统一鉴权平台，可在接口层补充租户、组织、权限隔离逻辑

---

## 十一、ADS 层血泪教训与实践规避

| 血泪教训                      | 典型后果                           | 规避实践                                                       |
| ----------------------------- | ---------------------------------- | -------------------------------------------------------------- |
| 直接拿 DWS 表给 BI 和接口共用 | 看板变更影响接口、接口变更影响看板 | 拆分看板型 ADS 与接口型 ADS                                    |
| ADS 只有结果没有解释字段      | 业务不接受推荐或预警               | 增加 `alert_reason / trigger_reason / hot_level / trend_label` |
| 接口不设计排序键与分页键      | 前端分页慢、重复数据多             | 明确时间字段、主键、默认排序规则                               |
| 所有筛选都压到运行时 SQL      | 高并发时查询抖动明显               | 提前在 ADS 中准备高频筛选字段和排序字段                        |
| 看板直接做复杂窗口或排名计算  | 图表查询慢、体验差                 | 在 ADS 中预先落榜单、等级、标签                                |
| 只关注热点榜单不关注库存承接  | 爆款建议脱离供应链现实             | 追单接口必须同步输出库存支撑标识                               |

---

## 十二、校验建议 SQL

```sql
-- 校验库存驾驶舱是否存在异常负库存
SELECT *
FROM ads.ads_inventory_dispatch_dashboard
WHERE atp_qty < -100;

-- 校验调拨建议是否正常产出
SELECT suggest_action, COUNT(*) AS cnt
FROM ads.ads_inventory_dispatch_api
GROUP BY suggest_action;

-- 校验爆款榜单是否存在重复排名
SELECT stat_hour, sku_id, COUNT(*) AS cnt
FROM ads.ads_hot_sku_dashboard
GROUP BY stat_hour, sku_id
HAVING COUNT(*) > 1;

-- 校验追单建议分层是否合理
SELECT reorder_level, COUNT(*) AS cnt
FROM ads.ads_hot_sku_reorder_api
GROUP BY reorder_level;
```

---

## 十三、ADS 之上异步物化视图参考

> 本节作为高频榜单和驾驶舱页面的性能优化参考项，用于在 ADS 主题表之上继续做 Doris 异步物化视图加速。

### 12.1 库存驾驶舱高频视图

```sql
CREATE MATERIALIZED VIEW mv_ads_inventory_dispatch_dashboard
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
    warehouse_id,
    sku_id,
    atp_qty,
    demand_qty,
    shortage_risk_flag,
    dispatch_priority_score,
    inventory_health_level
FROM ads.ads_inventory_dispatch_dashboard;
```

### 12.2 爆款榜单高频视图

```sql
CREATE MATERIALIZED VIEW mv_ads_hot_sku_dashboard_top
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
    sku_name,
    brand_name,
    product_type,
    hot_score,
    hot_level,
    trend_label,
    rank_no
FROM ads.ads_hot_sku_dashboard;
```

### 12.3 适用建议

- 库存驾驶舱首页、高频榜单页、领导大屏适合优先命中 ADS 物化视图
- 明细排查页仍建议回查 ADS 原表或下钻 DWS / DWD
- 物化视图粒度要围绕高频查询路径设计，不建议一次建过多

---

## 十四、落地建议

- ADS 结果集优先落 Doris 内部表，直接服务 Superset 与数据接口
- 对高频榜单、驾驶舱页面，可继续在 ADS 之上叠加异步物化视图
- Superset 统一消费 ADS 主题数据集，不再直接连接 DWD/DWS 大表
- 接口服务优先消费接口型 ADS 表，避免复用 BI 查询 SQL
