# 实时湖仓全链路OOTB操作验证文档

> 版本：V1（一期全链路联调版）  
> 适用范围：服装行业实时湖仓 `ODS -> DIM -> DWD -> DWS -> ADS` 全链路验证  
> 配套文档：
> - [全链路源头测试数据设计与生成方案.md](/d:/workspace/realtime-lakehouse/全链路源头测试数据设计与生成方案.md)
> - [实时湖仓数据字典.md](/d:/workspace/realtime-lakehouse/实时湖仓数据字典.md)
> - [实时湖仓维度建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓维度建模实现方案.md)
> - [实时湖仓事件流建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓事件流建模实现方案.md)
> - [实时湖仓DWD建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓DWD建模实现方案.md)
> - [实时湖仓DWS建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓DWS建模实现方案.md)
> - [实时湖仓ADS建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓ADS建模实现方案.md)

---

## 一、文档目标

本手册用于指导你按固定顺序，一次性完成实时湖仓全链路联调验证。  
目标不是单点验证某一层，而是完整确认：

1. 源头测试数据已经成功进入湖仓
2. 各层表与视图均已正常产出结果
3. 两大核心场景已经在 ADS 层形成可消费结果
4. 出现问题时可以快速定位在哪一层断链

---

## 二、验证范围

本次全链路验证覆盖以下对象：

- ODS 维度层
  - `cdc_m_*`
  - `ods_m_*_append`
  - `ods_m_*_history`
  - `ods_m_*_latest`
- ODS 事实层
  - `ods.ods_kafka_app_click`
  - `ods.ods_app_click_analyze`
  - `ods.ods_order_item_cdc`
  - `ods.ods_inventory_txn_cdc`
  - `ods.ods_api_logistics_track`
  - `ods.ods_api_logistics_track_analyze`
  - `ods.ods_social_mention`
  - `ods.ods_social_mention_analyze`
- DIM 层
  - `dim.dim_sku_wide`
- DWD 层
  - `dwd.dwd_order_trade_detail_rt`
  - `dwd.dwd_inventory_flow_detail_rt`
  - `dwd.dwd_logistics_track_detail_rt`
  - `dwd.dwd_user_behavior_detail_rt`
  - `dwd.dwd_social_mention_detail_rt`
  - `dwd.dwd_hot_sku_event_detail_rt`
- DWS 层
  - `dws.dws_inventory_atp_rt`
  - `dws.dws_inventory_dispatch_rt`
  - `dws.dws_hot_sku_feature_rt`
  - `dws.dws_hot_sku_rank_rt`
- ADS 层
  - `ads.ads_inventory_dispatch_dashboard`
  - `ads.ads_inventory_dispatch_api`
  - `ads.ads_hot_sku_dashboard`
  - `ads.ads_hot_sku_reorder_api`

---

## 三、前置检查

在开始联调前，先确认以下事项：

### 3.1 环境可用性

- MySQL 可连接
- Kafka 可连接
- Flink SQL Client 可用
- Paimon Catalog 可正常访问 HDFS 仓库
- Doris FE / BE 正常
- Superset 和数据接口这轮先不作为阻塞项，只验证 ADS 表是否产出

### 3.2 当前建议顺序

1. 先执行源头造数
2. 再核对 ODS
3. 再核对 DIM
4. 再核对 DWD
5. 再核对 DWS
6. 最后核对 ADS

不要一上来直接查 ADS。  
如果 ADS 没有数据，必须先回到 DWS 和 DWD 查断点。

### 3.3 业务批次约定

本次建议统一围绕 [全链路源头测试数据设计与生成方案.md](/d:/workspace/realtime-lakehouse/全链路源头测试数据设计与生成方案.md) 中的 `BATCH_20260403_V1` 批次进行验证。

重点 SKU：

- `P2026S001-BLK-M`
- `P2026S001-RED-S`
- `P2026S002-NVY-L`
- `P2026S001-NVY-XL`

---

## 四、整体操作顺序

### 4.1 主链路验证顺序

1. 执行 ERP 维度主数据造数
2. 验证 ODS 维度 Latest 表
3. 验证 `dim.dim_sku_wide`
4. 执行 ERP 订单与库存造数
5. 发送行为、物流、社媒 Kafka 消息
6. 验证 ODS 事实表与 Analyze 表
7. 验证 DWD 明细表
8. 验证 DWS 聚合表
9. 执行 ADS 刷新 SQL
10. 验证 ADS 结果表

### 4.2 增强链路验证顺序

主链路跑通后，再做：

1. Schema 漂移样本
2. 迟到数据样本
3. 重复数据样本

---

## 五、步骤1：执行源头测试数据

### 5.1 执行方式

直接按照 [全链路源头测试数据设计与生成方案.md](/d:/workspace/realtime-lakehouse/全链路源头测试数据设计与生成方案.md) 执行：

- 第 4 章：维度主数据
- 第 5 章：ERP 订单与库存
- 第 6 章：App 行为流
- 第 7 章：物流轨迹流
- 第 8 章：社媒舆情流

### 5.2 验证通过标准

- MySQL 插数成功，无主键冲突
- Kafka 消息发送成功，无格式报错
- 维度主数据、订单、库存、行为、物流、社媒均已写入

---

## 六、步骤2：验证 ODS 维度层

### 6.1 验证目标

确认维度 CDC 已成功进入 ODS，并形成 `latest` 镜像。

### 6.2 推荐检查 SQL

```sql
SELECT id, name, value, m_dim_id, m_attributesetinstance_id
FROM ods.ods_m_product_latest
ORDER BY id;

SELECT id, brand_name, season, wave_band, product_type, is_active
FROM ods.ods_m_dim_latest
ORDER BY id;

SELECT id, m_product_id, alias_type, alias_value
FROM ods.ods_m_product_alias_latest
ORDER BY id;

SELECT id, m_attributeset_id, description
FROM ods.ods_m_attributesetinstance_latest
ORDER BY id;

SELECT id, m_attributesetinstance_id, attribute_key, attribute_value
FROM ods.ods_m_attribute_latest
ORDER BY id;
```

### 6.3 预期结果

- `ods_m_product_latest`：应看到 5 个 SKU
- `ods_m_dim_latest`：应看到 3 个维度记录
- `ods_m_product_alias_latest`：应看到颜色和尺码别名
- `ods_m_attribute_latest`：应看到材质、产地等扩展属性

### 6.4 常见问题排查

- `latest` 表无数据
  - 检查 MySQL CDC 源表是否运行
  - 检查 Flink 维度同步任务是否已启动
- 数据不全
  - 检查是否先插入主表、后插入从表导致 CDC 尚未消费完成

---

## 七、步骤3：验证 DIM 层

### 7.1 验证目标

确认商品维度宽表 `dim.dim_sku_wide` 已完成退化打平，可供 DWD 补维。

### 7.2 推荐检查 SQL

```sql
SELECT
    sku_id,
    spu_code,
    sku_name,
    brand_name,
    season,
    wave_band,
    series,
    product_type,
    qdtype_pdt,
    color_code,
    size_code,
    material,
    origin
FROM dim.dim_sku_wide
ORDER BY sku_id;
```

### 7.3 预期结果

- 至少可查到 5 个 SKU
- `P2026S001-BLK-M`、`P2026S001-RED-S`、`P2026S002-NVY-L` 等均能看到中文展示字段
- 颜色、尺码、材质、产地等退化字段可直接使用

### 7.4 验证通过标准

- `sku_id` 唯一
- 所有后续联调涉及的 SKU 均已在 `dim_sku_wide` 中存在

---

## 八、步骤4：验证 ODS 事实层

### 8.1 行为 ODS

```sql
SELECT user_id, sku_id, behavior, _event_time, _partition_dt, _partition_hh
FROM ods.ods_kafka_app_click
ORDER BY _event_time;

SELECT user_id, sku_id, behavior, device_type, page_id, promotion_id, json_parse_status, _event_time
FROM ods.ods_app_click_analyze
ORDER BY _event_time;
```

预期：

- 行为主表保留原始入湖记录
- Analyze 表可兼容 `device` / `client_type`
- `promotion_id` 能解析出来

### 8.2 订单 ODS

```sql
SELECT order_id, line_no, user_id, sku_id, order_status, channel_code, qty, sale_price, sale_amount, order_time, pay_time, _event_time
FROM ods.ods_order_item_cdc
ORDER BY order_id, line_no;
```

预期：

- 应至少看到 6 条订单明细
- `sku_id` 与维度宽表可关联

### 8.3 库存 ODS

```sql
SELECT txn_id, sku_id, store_id, warehouse_id, biz_type, biz_no, qty, stock_status, biz_time, _event_time
FROM ods.ods_inventory_txn_cdc
ORDER BY biz_time, txn_id;
```

预期：

- 应至少看到 12 条库存流水
- `AVAILABLE / LOCKED / IN_TRANSIT` 均有样本

### 8.4 物流 ODS

```sql
SELECT logistics_no, order_id, sku_id, carrier_code, node_code, node_name, node_status, _event_time
FROM ods.ods_api_logistics_track
ORDER BY _event_time;

SELECT logistics_no, order_id, sku_id, node_code, node_name, province, city, courier, station_code, json_parse_status
FROM ods.ods_api_logistics_track_analyze
ORDER BY _event_time;
```

预期：

- 原始表和 Analyze 表均有数据
- 解析表可解析 `province/city/courier`

### 8.5 社媒 ODS

```sql
SELECT platform, note_id, author_id, sku_id, spu_code, like_cnt, fav_cnt, comment_cnt, _event_time
FROM ods.ods_social_mention
ORDER BY _event_time;

SELECT platform, note_id, sku_id, spu_code, keyword, scene_tag, creator_level, is_video, device_type, like_cnt, fav_cnt, comment_cnt, json_parse_status
FROM ods.ods_social_mention_analyze
ORDER BY _event_time;
```

预期：

- `P2026S001-BLK-M` 社媒热度样本较多
- `like_cnt / fav_cnt / comment_cnt` 已完整落地

### 8.6 验证通过标准

- 所有 ODS 事实表均有数据
- Analyze 表均有结果
- 关键字段 `sku_id`、`event_time`、分区字段正常

---

## 九、步骤5：验证 DWD 层

### 9.1 订单交易明细宽表

```sql
SELECT order_id, line_no, sku_id, spu_code, sku_name, brand_name, season, product_type, channel_code, store_id, order_status, qty, sale_price, sale_amount, event_time
FROM dwd.dwd_order_trade_detail_rt
ORDER BY event_time, order_id, line_no;
```

预期：

- 能看到订单明细已补齐商品维度字段
- `P2026S001-BLK-M`、`P2026S001-RED-S`、`P2026S002-NVY-L` 均有记录

### 9.2 库存流水明细宽表

```sql
SELECT txn_id, sku_id, store_id, warehouse_id, spu_code, sku_name, brand_name, season, product_type, biz_type, qty, stock_status, event_time
FROM dwd.dwd_inventory_flow_detail_rt
ORDER BY event_time, txn_id;
```

预期：

- 可查到库存流水已补齐商品维度字段
- `stock_status` 三种状态均存在

### 9.3 物流轨迹明细宽表

```sql
SELECT logistics_no, order_id, sku_id, spu_code, sku_name, brand_name, node_code, node_name, node_status, province, city, event_time
FROM dwd.dwd_logistics_track_detail_rt
ORDER BY event_time, logistics_no;
```

### 9.4 用户行为明细宽表

```sql
SELECT user_id, sku_id, spu_code, sku_name, brand_name, season, product_type, behavior, device_type, page_id, promotion_id, event_time
FROM dwd.dwd_user_behavior_detail_rt
ORDER BY event_time;
```

### 9.5 社媒舆情明细宽表

```sql
SELECT platform, note_id, sku_id, spu_code, brand_name, season, product_type, scene_tag, creator_level, is_video, like_cnt, fav_cnt, comment_cnt, event_time
FROM dwd.dwd_social_mention_detail_rt
ORDER BY event_time;
```

### 9.6 爆款事件归一化明细表

```sql
SELECT event_source, sku_id, spu_code, metric_type, metric_value, event_time
FROM dwd.dwd_hot_sku_event_detail_rt
ORDER BY event_time, event_source;
```

预期：

- 至少有 `ORDER`、行为类、社媒类三类事件
- `P2026S001-BLK-M` 的事件密度明显更高

### 9.7 验证通过标准

- 六张 DWD 明细表均有数据
- 维度补齐成功
- 爆款事件归一化成功

---

## 十、步骤6：验证 DWS 层

> 说明：DWS 在 Doris 中构建，验证时以 Doris 查询结果为准。

### 10.1 库存 ATP 汇总

```sql
SELECT stat_minute, store_id, warehouse_id, sku_id, available_qty, locked_qty, in_transit_qty, stock_delta_qty, atp_qty
FROM dws.dws_inventory_atp_rt
ORDER BY stat_minute, sku_id;
```

预期：

- `P2026S001-RED-S` 的 `atp_qty` 应偏低
- `P2026S002-NVY-L` 的 `atp_qty` 应明显更健康

### 10.2 库存调拨建议汇总

```sql
SELECT stat_minute, store_id, sku_id, atp_qty, order_qty, shortage_risk_flag, dispatch_priority_score
FROM dws.dws_inventory_dispatch_rt
ORDER BY stat_minute, dispatch_priority_score DESC;
```

预期：

- `P2026S001-RED-S` 应触发 `shortage_risk_flag = 1`
- 存在调拨优先级分值

### 10.3 爆款特征汇总

```sql
SELECT stat_hour, sku_id, sale_amount_1h, order_cnt_1h, view_cnt_1h, click_cnt_1h, add_cart_cnt_1h, social_hot_1h, hot_score
FROM dws.dws_hot_sku_feature_rt
ORDER BY stat_hour, hot_score DESC;
```

预期：

- `P2026S001-BLK-M` 热度分值最高或靠前
- `sale_amount_1h`、`order_cnt_1h`、`social_hot_1h` 均有贡献

### 10.4 爆款排行汇总

```sql
SELECT stat_hour, brand_name, season, product_type, sku_id, hot_score, rank_no, reorder_suggest_flag
FROM dws.dws_hot_sku_rank_rt
ORDER BY stat_hour, rank_no;
```

预期：

- 应产出排名
- 爆款 SKU 应具备追单建议标识

### 10.5 验证通过标准

- 四张 DWS 表均有结果
- 两大主题场景都已形成聚合产物

---

## 十一、步骤7：执行并验证 ADS 层

### 11.1 执行 ADS 刷新 SQL

按 [实时湖仓ADS建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓ADS建模实现方案.md) 第 7 章顺序执行：

1. `ads_inventory_dispatch_dashboard`
2. `ads_inventory_dispatch_api`
3. `ads_hot_sku_dashboard`
4. `ads_hot_sku_reorder_api`

### 11.2 验证库存驾驶舱 ADS

```sql
SELECT stat_minute, store_id, warehouse_id, sku_id, atp_qty, demand_qty, shortage_risk_flag, dispatch_priority_score, inventory_health_level, alert_reason
FROM ads.ads_inventory_dispatch_dashboard
ORDER BY stat_minute, dispatch_priority_score DESC;
```

预期：

- 可以看到健康等级
- 可以看到缺货原因
- 可以看到调拨优先级

### 11.3 验证调拨接口 ADS

```sql
SELECT stat_minute, store_id, warehouse_id, sku_id, atp_qty, demand_qty, shortage_risk_flag, dispatch_priority_score, suggest_action, trigger_reason
FROM ads.ads_inventory_dispatch_api
ORDER BY stat_minute, dispatch_priority_score DESC;
```

预期：

- `P2026S001-RED-S` 应触发 `NORMAL_DISPATCH` 或 `URGENT_DISPATCH`

### 11.4 验证爆款看板 ADS

```sql
SELECT stat_hour, sku_id, hot_score, hot_level, trend_label, rank_no, sale_amount_1h, social_hot_1h
FROM ads.ads_hot_sku_dashboard
ORDER BY stat_hour, rank_no;
```

预期：

- `P2026S001-BLK-M` 应热度更高
- 有 `hot_level` 和 `trend_label`

### 11.5 验证追单接口 ADS

```sql
SELECT stat_hour, sku_id, hot_score, rank_no, reorder_suggest_flag, reorder_level, trigger_reason, stock_support_flag
FROM ads.ads_hot_sku_reorder_api
ORDER BY stat_hour, rank_no;
```

预期：

- 爆款 SKU 具备追单建议
- 有可解释字段 `trigger_reason`

### 11.6 验证通过标准

- 四张 ADS 表均有结果
- 库存和爆款两大主题均已可消费

---

## 十二、步骤8：增强样本验证

> 说明：这一步建议主链路完全通过后再做。

### 12.1 Schema 漂移

发送增强样本后检查：

```sql
SELECT logistics_no, node_code, station_code, json_parse_status
FROM ods.ods_api_logistics_track_analyze
WHERE logistics_no = 'SF202604030004';

SELECT note_id, creator_level, is_video, json_parse_status
FROM ods.ods_social_mention_analyze
WHERE note_id = 'XHS202604030005';
```

预期：

- 新增字段可被解析
- 不需要重建原始 ODS 表

### 12.2 迟到数据

```sql
SELECT logistics_no, node_code, _event_time, _partition_dt, _partition_hh
FROM ods.ods_api_logistics_track
WHERE logistics_no = 'SF202604030001'
ORDER BY _event_time;
```

预期：

- 分区应按事件时间路由
- 下游统计应回补到正确窗口

### 12.3 重复数据

```sql
SELECT note_id, COUNT(*)
FROM ods.ods_social_mention
WHERE note_id = 'XHS202604030006'
GROUP BY note_id;
```

预期：

- 可识别重复样本
- 不应导致下游爆款热度异常失真

---

## 十三、常见问题排查顺序

### 13.1 ADS 没有数据

按顺序查：

1. ADS 刷新 SQL 是否执行
2. DWS 是否已有结果
3. DWD 是否已有明细
4. ODS 是否已有源数据

### 13.2 DWS 没有数据

按顺序查：

1. DWD 是否已有数据
2. DWD 表是否带 `WATERMARK`
3. Flink 窗口 SQL 是否已启动
4. Doris Sink 是否可写

### 13.3 DWD 没有数据

按顺序查：

1. ODS 事实表是否有数据
2. `dim.dim_sku_wide` 是否有对应 SKU
3. Join 字段类型是否一致
4. 时间字段和分区字段是否正常

### 13.4 DIM 没有数据

按顺序查：

1. `ods_m_*_latest` 是否有数据
2. 维度宽表写入 SQL 是否执行
3. 别名和属性透视视图是否正常

### 13.5 ODS 没有数据

按顺序查：

1. MySQL CDC / Kafka Source 是否已创建
2. Flink SQL Job 是否在运行
3. 源头造数是否真正发送成功

---

## 十四、最终验证通过标准

如果以下条件都满足，则本次“一期全链路联调”主链路可判定通过：

1. 源头维度、订单、库存、行为、物流、社媒数据均已成功写入
2. ODS 维度层与 ODS 事实层均有结果
3. `dim.dim_sku_wide` 已成功产出
4. 六张 DWD 明细表均已成功产出
5. 四张 DWS 聚合表均已成功产出
6. 四张 ADS 结果表均已成功产出
7. 库存场景已看到缺货风险和调拨建议
8. 爆款场景已看到热度、排行和追单建议

---

## 十五、建议的验证记录方式

建议你按以下格式记录每一步结果：

| 步骤 | 对象 | 是否通过 | 核心现象 | 备注 |
|---|---|---|---|---|
| ODS 维度 | `ods_m_product_latest` | 是/否 | 是否有 5 个 SKU |  |
| DIM | `dim_sku_wide` | 是/否 | 是否补齐商品维度 |  |
| DWD | `dwd_order_trade_detail_rt` | 是/否 | 是否可见订单宽表 |  |
| DWS | `dws_inventory_dispatch_rt` | 是/否 | 是否触发缺货风险 |  |
| ADS | `ads_hot_sku_reorder_api` | 是/否 | 是否形成追单建议 |  |

---

## 十六、下一步建议

完成本手册验证后，建议立即进入：

1. 汇总全链路问题清单
2. 统一修正文档与 SQL
3. 再补充数据治理和监控运维方案
4. 进入一期项目交付与验收阶段

