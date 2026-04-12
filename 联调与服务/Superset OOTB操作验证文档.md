# Superset OOTB操作验证文档

> 适用范围：基于 [实时湖仓ADS建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓ADS建模实现方案.md) 中的 Superset 消费口径设计，对 ADS 结果表进行开箱即用的操作验证。  
> 当前目标：你可以直接按照本文档在 Superset 中完成数据源接入、数据集创建、图表创建、看板组装与结果校验。  
> 验证边界：仅验证 Superset 如何消费 ADS 结果，不涉及 Superset 部署安装。

---

## 一、验证目标

本次 OOTB 验证要完成以下 4 件事：

1. 验证 Superset 能正常连接 Doris `ads` 库
2. 验证 ADS 主题表可正常创建为 Superset 数据集
3. 验证库存驾驶舱与爆款趋势看板的核心图表可正常出图
4. 验证筛选、排序、时间过滤、钻取入口是否符合设计预期

---

## 二、验证对象

### 2.1 本次验证使用的 ADS 表

| 场景 | ADS 表 | 用途 |
|---|---|---|
| 库存可视与调拨 | `ads.ads_inventory_dispatch_dashboard` | 库存驾驶舱数据集 |
| 爆款趋势识别 | `ads.ads_hot_sku_dashboard` | 爆款趋势看板数据集 |

### 2.2 本次验证使用的 Superset 数据集名称

| 数据集名称 | 来源表 |
|---|---|
| `dataset_inventory_dispatch_dashboard` | `ads.ads_inventory_dispatch_dashboard` |
| `dataset_hot_sku_dashboard` | `ads.ads_hot_sku_dashboard` |

---

## 三、前置检查

在开始操作前，先确认以下前置条件：

### 3.1 数据前置条件

- Doris 中已存在 `ads` 数据库
- 以下表已完成建表并有可查询数据：
  - `ads.ads_inventory_dispatch_dashboard`
  - `ads.ads_hot_sku_dashboard`
- 建议先在 Doris 中执行以下 SQL，确认表内已有数据：

```sql
SELECT COUNT(*) FROM ads.ads_inventory_dispatch_dashboard;
SELECT COUNT(*) FROM ads.ads_hot_sku_dashboard;
```

### 3.2 Superset 前置条件

- Superset 已可正常访问
- 你拥有创建 Database、Dataset、Chart、Dashboard 的权限
- 若 Doris 数据源尚未在 Superset 中配置，需要先完成一次数据库连接创建

### 3.3 推荐的 Doris 连接信息

如果需要在 Superset 中新建 Doris 数据源，可参考：

| 项目 | 示例值 |
|---|---|
| Host | `192.168.63.128` |
| Port | `9030` |
| Database | `ads` |
| Username | `root` |
| Password | 按当前环境实际填写 |

如果使用 SQLAlchemy URI，可参考：

```text
mysql+pymysql://root:<password>@192.168.63.128:9030/ads?charset=utf8mb4
```

> 说明：Superset 连接 Doris 查询端通常使用 `9030`，这和 Flink Doris Connector 写入时使用的 `8030` 不是一回事。

---

## 四、操作路径总览

你在 Superset 中本次主要会操作 4 个区域：

1. `Settings -> Database Connections`
2. `Data -> Datasets`
3. `Charts`
4. `Dashboards`

建议按以下顺序执行：

1. 创建或检查 Doris Database Connection
2. 创建两个 ADS Dataset
3. 分别创建库存和爆款图表
4. 组装成两个 Dashboard
5. 执行筛选、排序、刷新与结果核对

---

## 五、Step 1：创建或检查 Doris 数据源连接

### 5.1 操作步骤

1. 进入 `Settings -> Database Connections`
2. 点击右上角 `+ Database`
3. 选择 `MySQL` 兼容方式接入 Doris
4. 填写连接参数：
   - Host：`192.168.63.128`
   - Port：`9030`
   - Database Name：`ads`
   - Username：`root`
   - Password：按环境填写
5. 点击 `Test Connection`
6. 测试通过后点击 `Connect`

### 5.2 预期结果

- 数据源连接测试成功
- Superset 中可看到 `ads` 库
- 进入 SQL Lab 时可直接查询 `ads` 下的表

### 5.3 建议先做一次 SQL Lab 验证

进入 `SQL Lab`，执行：

```sql
SELECT *
FROM ads.ads_inventory_dispatch_dashboard
LIMIT 10;
```

如果能正常返回记录，说明 Superset 到 Doris 的查询链路正常。

---

## 六、Step 2：创建 ADS 数据集

### 6.1 创建库存驾驶舱数据集

1. 进入 `Data -> Datasets`
2. 点击 `+ Dataset`
3. 选择刚创建好的 Doris 数据源
4. Schema 选择：`ads`
5. Table 选择：`ads_inventory_dispatch_dashboard`
6. 保存为：`dataset_inventory_dispatch_dashboard`

### 6.2 创建爆款趋势数据集

1. 再次点击 `+ Dataset`
2. 选择 Doris 数据源
3. Schema 选择：`ads`
4. Table 选择：`ads_hot_sku_dashboard`
5. 保存为：`dataset_hot_sku_dashboard`

### 6.3 数据集字段检查

创建完成后，分别打开两个 Dataset，确认字段存在：

#### 库存数据集应重点看到

- `stat_minute`
- `store_id`
- `warehouse_id`
- `sku_id`
- `sku_name`
- `brand_name`
- `season`
- `product_type`
- `available_qty`
- `locked_qty`
- `in_transit_qty`
- `atp_qty`
- `demand_qty`
- `shortage_risk_flag`
- `dispatch_priority_score`
- `inventory_health_level`

#### 爆款数据集应重点看到

- `stat_hour`
- `sku_id`
- `sku_name`
- `brand_name`
- `season`
- `wave_band`
- `series`
- `product_type`
- `sale_amount_1h`
- `order_cnt_1h`
- `view_cnt_1h`
- `click_cnt_1h`
- `add_cart_cnt_1h`
- `social_hot_1h`
- `hot_score`
- `hot_level`
- `trend_label`
- `rank_no`

---

## 七、Step 3：库存驾驶舱图表验证

### 7.1 图表一：店仓 ATP 热力图

#### 推荐配置

- Dataset：`dataset_inventory_dispatch_dashboard`
- Chart Type：`Heatmap`
- X Axis：`store_id`
- Y Axis：`warehouse_id`
- Metric：`SUM(atp_qty)`
- Time Column：`stat_minute`
- Time Grain：`Minute`

#### 预期结果

- 可以看到不同店仓的 ATP 分布
- ATP 高低区域有明显颜色差异
- 更换时间范围后结果会同步变化

### 7.2 图表二：缺货风险排行榜

#### 推荐配置

- Dataset：`dataset_inventory_dispatch_dashboard`
- Chart Type：`Table`
- Columns：
  - `sku_name`
  - `store_id`
  - `warehouse_id`
  - `atp_qty`
  - `demand_qty`
  - `dispatch_priority_score`
  - `inventory_health_level`
- Filters：
  - `shortage_risk_flag = 1`
- Sort By：
  - `dispatch_priority_score DESC`

#### 预期结果

- 表格中优先展示高优先级缺货商品
- 排序结果应与 ADS 设计口径一致
- 同一商品在不同店仓可分开展示

### 7.3 图表三：在途与锁定库存对比

#### 推荐配置

- Dataset：`dataset_inventory_dispatch_dashboard`
- Chart Type：`Bar Chart`
- X Axis：`sku_name`
- Metrics：
  - `SUM(in_transit_qty)`
  - `SUM(locked_qty)`
- Filters：可按 `brand_name / season / product_type` 过滤

#### 预期结果

- 能看出在途和锁定库存的相对规模
- 不同筛选条件下结果会联动变化

### 7.4 图表四：库存健康分层

#### 推荐配置

- Dataset：`dataset_inventory_dispatch_dashboard`
- Chart Type：`Pie Chart`
- Group By：`inventory_health_level`
- Metric：`COUNT(*)`

#### 预期结果

- 能直观看出 `GREEN / ORANGE / RED` 分层比例
- 颜色建议手动设置为绿、橙、红，便于业务理解

---

## 八、Step 4：爆款趋势看板图表验证

### 8.1 图表一：爆款 TopN 榜单

#### 推荐配置

- Dataset：`dataset_hot_sku_dashboard`
- Chart Type：`Table`
- Columns：
  - `rank_no`
  - `sku_name`
  - `brand_name`
  - `product_type`
  - `hot_score`
  - `hot_level`
  - `trend_label`
- Sort By：
  - `hot_score DESC`
- Row Limit：`20`

#### 预期结果

- 榜单能稳定展示前 20 个高热度 SKU
- `rank_no` 与 `hot_score` 排序结果应基本一致

### 8.2 图表二：热度趋势折线图

#### 推荐配置

- Dataset：`dataset_hot_sku_dashboard`
- Chart Type：`Time-series Line Chart`
- Time Column：`stat_hour`
- Time Grain：`Hour`
- Metric：`AVG(hot_score)` 或 `SUM(hot_score)`
- Group By：可选 `sku_name` 或 `brand_name`

#### 预期结果

- 能看到小时级热度变化趋势
- 过滤不同品牌/品类后，曲线会随之变化

### 8.3 图表三：销量/社媒双轴图

#### 推荐配置

- Dataset：`dataset_hot_sku_dashboard`
- Chart Type：`Mixed Time-series`
- Time Column：`stat_hour`
- Metrics：
  - `SUM(sale_amount_1h)`
  - `SUM(social_hot_1h)`

#### 预期结果

- 能同时看销量与社媒热度变化
- 可用于观察“销量是否跟随热度同步变化”

### 8.4 图表四：品类爆款分布

#### 推荐配置

- Dataset：`dataset_hot_sku_dashboard`
- Chart Type：`Bar Chart`
- X Axis：`product_type`
- Group By：`hot_level`
- Metric：`COUNT(*)`

#### 预期结果

- 可识别不同品类的爆款等级分布
- 更适合管理层看品类结构

---

## 九、Step 5：创建两个 Dashboard

### 9.1 库存驾驶舱 Dashboard

#### 建议名称

`dashboard_inventory_dispatch`

#### 推荐图表布局

1. 第一行：
   - 店仓 ATP 热力图
   - 库存健康分层
2. 第二行：
   - 缺货风险排行榜
3. 第三行：
   - 在途与锁定库存对比

#### 推荐全局筛选器

- `stat_minute`
- `brand_name`
- `season`
- `product_type`
- `store_id`

### 9.2 爆款趋势 Dashboard

#### 建议名称

`dashboard_hot_sku_trend`

#### 推荐图表布局

1. 第一行：
   - 爆款 TopN 榜单
   - 品类爆款分布
2. 第二行：
   - 热度趋势折线图
3. 第三行：
   - 销量/社媒双轴图

#### 推荐全局筛选器

- `stat_hour`
- `brand_name`
- `season`
- `product_type`
- `hot_level`

---

## 十、Step 6：验证筛选、排序与口径

### 10.1 库存看板验证项

| 验证项 | 操作方式 | 预期结果 |
|---|---|---|
| 时间过滤 | 调整 `stat_minute` 时间范围 | 图表数据同步变化 |
| 高风险排序 | 排行榜按 `dispatch_priority_score DESC` | 高优先级商品排在前面 |
| 品牌过滤 | 选择某个 `brand_name` | 只展示对应品牌数据 |
| 健康分层 | 切换时间窗口 | 红橙绿比例发生变化 |

### 10.2 爆款看板验证项

| 验证项 | 操作方式 | 预期结果 |
|---|---|---|
| 时间过滤 | 调整 `stat_hour` | 趋势图和榜单联动 |
| 热度排序 | 按 `hot_score DESC` 查看榜单 | 排名稳定且可解释 |
| 品类过滤 | 选择 `product_type` | 只展示对应品类 |
| 等级分布 | 观察 `hot_level` 变化 | 不同品类热度层次可区分 |

---

## 十一、结果核对建议

建议在 Superset 与 Doris 查询结果之间做一次抽样核对：

### 11.1 库存主题抽样核对

```sql
SELECT
    stat_minute,
    store_id,
    warehouse_id,
    sku_id,
    atp_qty,
    demand_qty,
    dispatch_priority_score
FROM ads.ads_inventory_dispatch_dashboard
ORDER BY dispatch_priority_score DESC
LIMIT 20;
```

核对点：

- Superset 排行榜前 20 条是否与 SQL 查询一致
- ATP、需求量、优先级分值是否一致

### 11.2 爆款主题抽样核对

```sql
SELECT
    stat_hour,
    sku_id,
    sku_name,
    hot_score,
    rank_no,
    hot_level
FROM ads.ads_hot_sku_dashboard
ORDER BY hot_score DESC
LIMIT 20;
```

核对点：

- Superset 爆款 TopN 榜单是否与 SQL 查询一致
- `hot_score / rank_no / hot_level` 是否一致

---

## 十二、常见问题排查

### 12.1 数据集创建后看不到表

可能原因：

- Superset 连接的不是 `ads` 库
- Doris 账号没有访问 `ads` 库权限
- Metadata 没刷新

建议处理：

1. 重新检查 Database Connection
2. 在 SQL Lab 执行 `SHOW TABLES FROM ads`
3. 必要时重新同步 Superset 元数据

### 12.2 图表无数据

可能原因：

- ADS 表本身没有数据
- 时间过滤范围过窄
- 默认筛选条件过严

建议处理：

1. 先在 Doris 里执行 `COUNT(*)`
2. 取消图表过滤条件再看一次
3. 把时间范围调大到最近 1 天

### 12.3 图表排序不符合预期

可能原因：

- 排序字段没显式配置
- 使用了错误的指标聚合方式

建议处理：

1. 再次确认排序字段：
   - 库存：`dispatch_priority_score`
   - 爆款：`hot_score`
2. 表格图优先用原字段排序，不要误用聚合后的别名

### 12.4 数据与 SQL Lab 不一致

可能原因：

- 图表缓存未刷新
- 图表用了不同的时间粒度或过滤条件

建议处理：

1. 强制刷新图表
2. 对照图表配置核对 Time Grain、Filters、Sort By

---

## 十三、验证通过标准

满足以下条件即可认为本次 Superset OOTB 验证通过：

- Doris 数据源连接成功
- 两个 ADS 数据集创建成功
- 两类场景的核心图表均能正常出图
- 时间筛选、品牌筛选、排序规则符合预期
- Superset 图表抽样结果与 Doris SQL 结果一致
- Dashboard 可保存并刷新，无明显性能异常

---

## 十四、建议的验证记录方式

建议你在验证时同步记录以下信息：

- 验证时间
- 验证人
- 使用的数据时间范围
- 每个图表是否成功出图
- 是否存在排序/筛选/口径异常
- 是否需要回到 ADS 文档修正字段或 SQL

如果你愿意，后面我还可以继续帮你补一版“Superset 验证记录模板”，让你直接按表格填结果。 

