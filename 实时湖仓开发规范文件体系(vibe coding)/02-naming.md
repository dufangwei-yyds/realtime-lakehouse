# 命名规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：表名、作业名、Topic 名、Kafka 消费者组等命名规则  
> 适用范围：ODS/DIM/DWD/DWS/ADS 全链路命名、Flink 作业命名、Kafka Topic 命名

---

## 一、命名基本原则

### 1.1 命名通用规则

| 规则 | 说明 | 示例 |
|-----|------|------|
| 全部小写 | 统一使用小写字母 | ? ods_order_detail / ? ODS_Order_Detail |
| 使用下划线分隔 | 层级之间用下划线分隔 | ? dim_sku_wide / ? dim-sku-wide |
| 禁止中文 | 所有命名不得使用中文 | - |
| 禁止特殊字符 | 仅允许字母、数字、下划线 | - |
| 语义清晰 | 名称必须能表达业务含义 | ? ods_sale_order_cdc / ? ods_t1 |
| 有界长度 | 单段不超过 30 字符 | - |

### 1.2 命名层级前缀

| 层级 | 前缀 | 说明 |
|-----|------|------|
| ODS 层 | ods_ | 操作数据存储层 |
| DIM 层 | dim_ | 维度层 |
| DWD 层 | dwd_ | 明细数据层 |
| DWS 层 | dws_ | 汇总数据层 |
| ADS 层 | ads_ | 应用数据层 |
| CDC 源表 | cdc_ | CDC 接入临时表 |
| 外部表 | ext_ | Doris 外部表 |

---

## 二、分层对象命名

### 2.1 ODS 层命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| CDC 源表 | cdc_{table} | cdc_sale_order |
| 维度追加表 | ods_{table}_append | ods_m_product_append |
| 维度历史表 | ods_{table}_history | ods_m_product_history |
| 维度最新镜像表 | ods_{table}_latest | ods_m_product_latest |
| 事件解析表 | ods_{domain}_{event}_analyze | ods_app_click_analyze |
| 订单事实表 | ods_{source}_order | ods_kafka_order |
| 库存流水事实表 | ods_{source}_inventory_txn | ods_cdc_inventory_txn |
| 物流轨迹事实表 | ods_api_logistics_track | ods_api_logistics_track |
| 社媒事实表 | ods_social_mention | ods_social_mention |
| 去重表 | ods_{source}_dedup | ods_kafka_app_click_dedup |

### 2.2 DIM 层命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| 维度宽表 | dim_{domain}_wide | dim_sku_wide |
| 维度追加表 | dim_{domain}_{entity}_append | dim_product_append |
| 维度历史表 | dim_{domain}_{entity}_history | dim_product_history |
| 维度最新表 | dim_{domain}_{entity}_latest | dim_product_latest |

### 2.3 DWD 层命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| 交易明细表 | dwd_{domain}_trade_detail_rt | dwd_order_trade_detail_rt |
| 库存明细表 | dwd_{domain}_inventory_detail_rt | dwd_inventory_detail_rt |
| 行为明细表 | dwd_{domain}_behavior_detail_rt | dwd_app_behavior_detail_rt |
| 物流明细表 | dwd_{domain}_logistics_detail_rt | dwd_logistics_detail_rt |
| 社媒明细表 | dwd_{domain}_social_detail_rt | dwd_social_mention_detail_rt |

### 2.4 DWS 层命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| 主题汇总表 | dws_{theme}_{grain}_rt | dws_inventory_atp_rt |
| 粒度汇总表 | dws_{theme}_{agg_type}_{grain}_rt | dws_sales_hh_sku_rt |

### 2.5 ADS 层命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| Dashboard 结果表 | ads_{theme}_dashboard | ads_inventory_dashboard |
| API 结果表 | ads_{theme}_api | ads_hot_sku_api |
| 物化视图 | ads_{theme}_{view}_mv | ads_sales_summary_mv |

---

## 三、Kafka 命名规范

### 3.1 Topic 命名

**命名规范**
```
rt_{domain}_{object}_{event}
```

| 组成部分 | 说明 | 取值示例 |
|---------|------|---------|
| rt_ | 前缀，表示实时主题 | rt_ |
| domain | 业务域 | trade / behavior / logistics / social |
| object | 对象/实体 | order / click / track / mention |
| event | 事件类型 | created / click / signed / published |

**示例**

| Topic 名 | 业务含义 |
|---------|---------|
| rt_trade_order_created | 交易域-订单-已创建事件 |
| rt_behavior_app_click | 行为域-APP-点击事件 |
| rt_logistics_track_update | 物流域-轨迹-更新事件 |
| rt_social_xhs_note | 社媒域-小红书-笔记事件 |

### 3.2 消费者组命名

**命名规范**
```
flink_{layer}_{purpose}_consumer
```

**示例**

| 消费者组 | 用途 |
|---------|------|
| flink_ods_app_click_consumer | ODS 层消费 APP 点击流 |
| flink_ods_order_consumer | ODS 层消费订单流 |
| flink_dwd_trade_consumer | DWD 层消费交易明细 |
| flink_dws_inventory_consumer | DWS 层消费库存汇总 |

---

## 四、Flink 作业命名

### 4.1 作业命名规范

**命名规范**
```
job_{domain}_{purpose}_{env}
```

| 组成部分 | 说明 | 取值示例 |
|---------|------|---------|
| job_ | 前缀 | job_ |
| domain | 业务域 | product / order / inventory / behavior |
| purpose | 作业目的 | cdc / ods / dim / dwd / dws |
| env | 环境 | sit / uat / prod |

**示例**

| 作业名 | 业务含义 |
|-------|---------|
| job_product_dim_prod | 商品域 DIM 作业-生产环境 |
| job_order_dwd_sit | 订单域 DWD 作业-测试环境 |
| job_inventory_dws_prod | 库存域 DWS 作业-生产环境 |
| job_behavior_ods_sit | 行为域 ODS 作业-测试环境 |

### 4.2 作业类型后缀

| 后缀 | 含义 | 使用场景 |
|-----|------|---------|
| _cdc | CDC 接入作业 | ODS 层接入业务库 |
| _ods | ODS 层作业 | 事件流清洗 |
| _dim | DIM 层作业 | 维度构建 |
| _dwd | DWD 层作业 | 明细打宽 |
| _dws | DWS 层作业 | 汇总聚合 |
| _scd2 | SCD2 专项作业 | 维度历史变化 |

---

## 五、Paimon 对象命名

### 5.1 Catalog 和 Database 命名

| 对象 | 命名规范 | 示例 |
|-----|---------|------|
| Catalog | paimon_catalog | - |
| ODS Database | ods | - |
| DIM Database | dim | - |
| DWD Database | dwd | - |
| DWS Database | dws | - |
| ADS Database | ads | - |

### 5.2 表命名规则

**Paimon 表命名遵循分层对象命名规范，无需额外后缀。**

示例：
- Paimon Catalog: paimon_catalog
- Database: ods
- Table: ods_kafka_app_click

完整路径：`paimon_catalog.ods.ods_kafka_app_click`

---

## 六、Doris 对象命名

### 6.1 内部表命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| ADS 结果表 | ads_{theme}_{consumer} | ads_hot_sku_dashboard |
| 外部表 | ads_{source}_ext | ads_paimon_dwd_ext |
| 物化视图 | ads_{theme}_{view}_mv | ads_sales_daily_mv |

### 6.2 资源命名

| 对象类型 | 命名规范 | 示例 |
|---------|---------|------|
| JDBC Resource | paimon_resource | - |

---

## 七、字段命名规范

### 7.1 通用字段命名

| 字段类型 | 命名规范 | 示例 |
|---------|---------|------|
| 业务主键 | {entity}_id | order_id / sku_id / user_id |
| 外键 | {entity}_id | store_id / warehouse_id |
| 业务时间 | {entity}_time | order_time / pay_time / biz_time |
| 金额 | {entity}_amount | sale_amount / order_amount |
| 数量 | {entity}_qty | qty / sale_qty |
| 状态 | {entity}_status | order_status / stock_status |

### 7.2 元数据字段命名

| 字段名 | 含义 | 命名规则 |
|-------|------|---------|
| _event_time | 业务事件时间 | 固定 |
| _ingest_time | 入湖/入层时间 | 固定 |
| _partition_dt | 日期分区 | 固定，格式 yyyyMMdd |
| _partition_hh | 小时分区 | 固定，格式 HH |
| _kafka_offset | Kafka 偏移量 | 固定 |
| _kafka_partition | Kafka 分区号 | 固定 |

### 7.3 枚举值命名

| 枚举类型 | 取值规范 | 示例 |
|---------|---------|------|
| 订单状态 | 大写下划线 | CREATED / PAID / SHIPPED |
| 渠道编码 | 大写下划线 | EC / STORE / MINIAPP |
| 库存业务类型 | 大写下划线 | PUR_IN / SALE_OUT / TRANSFER |
| 库存状态 | 大写下划线 | AVAILABLE / LOCKED / IN_TRANSIT |

---

## 八、命名检查清单

### 8.1 自检项

生成命名后，必须检查：

- [ ] 全部小写，无大写字母
- [ ] 使用下划线分隔
- [ ] 无中文、无特殊字符
- [ ] 符合分层前缀规范
- [ ] 语义清晰，能表达业务含义
- [ ] 单段不超过 30 字符
- [ ] 已同步到数据字典

### 8.2 常见错误

| 错误类型 | 错误示例 | 正确示例 |
|---------|---------|---------|
| 大写下划线混用 | ods_Sale_Order | ods_sale_order |
| 中划线分隔 | dim-sku-wide | dim_sku_wide |
| 中文命名 | ods_订单表 | ods_sale_order |
| 语义不清 | ods_t1 | ods_sale_order_item |
| 缺少前缀 | sale_order | ods_sale_order |

---

## 九、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 04-field.md | 字段与数据类型规范 |
| 06-flink-sql.md | Flink SQL 开发规范 |

---

> 本文档定义了实时湖仓全链路命名规范，是表名、作业名、Topic 名等命名的核心参考。
