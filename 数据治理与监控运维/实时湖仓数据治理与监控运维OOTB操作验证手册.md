# 实时湖仓数据治理与监控运维OOTB操作验证手册

> 文档定位：实时湖仓生产最佳实践 OOTB 操作手册  
> 适用范围：`ODS -> DIM -> DWD -> DWS -> ADS` 全链路日常治理、监控、巡检、发布验证  
> 配套文档：
> - [实时湖仓数据治理与监控运维方案.md](/d:/workspace/realtime-lakehouse/实时湖仓数据治理与监控运维方案.md)
> - [实时湖仓全链路数据质量与监控预警模板.md](/d:/workspace/realtime-lakehouse/实时湖仓全链路数据质量与监控预警模板.md)
> - [实时湖仓数据字典.md](/d:/workspace/realtime-lakehouse/实时湖仓数据字典.md)
> - [实时湖仓测试环境部署方案.md](/d:/workspace/realtime-lakehouse/实时湖仓测试环境部署方案.md)

---

## 一、手册目标

本手册用于把当前已经形成的治理维度、质量规则、监控体系和全链路联调成果，落成一份“可以定期执行”的日常操作手册。

使用目标：

1. 支撑日常数据治理检查
2. 支撑日常作业监控和值守
3. 支撑发布前后核查
4. 支撑异常问题快速定位
5. 支撑一期项目收口后的持续维护

本手册强调：

- 可执行
- 可复用
- 可记录
- 可交接

---

## 二、适用对象

| 角色 | 主要用途 |
|---|---|
| 数据开发 | 日常质量检查、链路排障、补数前核查 |
| 运维 | 作业存活、容量、告警、发布与回滚执行 |
| 测试 | 回归验证、发布后重点检查 |
| BI / 接口支持 | ADS 结果、刷新时效、消费层核对 |
| 项目负责人 | 周期性复盘与验收前巡检 |

---

## 三、日常值守节奏

### 3.1 建议值守频率

| 频率 | 重点 |
|---|---|
| 每日 | 作业状态、延迟、质量异常、ADS 刷新、接口 SLA |
| 每周 | 容量、快照、小文件、物化视图、规则覆盖率 |
| 每月 | 口径复盘、文档一致性、问题闭环、治理策略优化 |
| 发布前后 | 全链路重点核查、差异比对、回滚准备 |

### 3.2 建议执行顺序

1. 先看作业和服务是否存活
2. 再看数据是否按时产出
3. 再看质量规则是否异常
4. 最后看消费层是否可用

---

## 四、每日治理与监控操作清单

## 4.1 步骤1：基础服务健康检查

### 检查目标

确认基础组件可用，避免把服务故障误判为数据问题。

### 必查服务

- MySQL
- Kafka
- Flink
- HDFS
- Doris
- Superset
- Prometheus / Grafana

### 建议检查项

| 检查项 | 通过标准 |
|---|---|
| Flink Web UI | 可访问 |
| Doris FE / BE | 正常存活 |
| Kafka Broker | 正常监听 |
| HDFS | 可读写 |
| Prometheus | 可查询指标 |
| Superset | 可访问登录页 |

### 推荐命令

```bash
ss -lntp | grep 9092
ss -lntp | grep 8081
ss -lntp | grep 9030
ss -lntp | grep 8888
```

### 页面检查

- `http://192.168.63.128:8081`
- `http://192.168.63.128:7030`
- `http://192.168.63.128:7040`
- `http://192.168.63.128:9090`
- `http://192.168.63.128:8888`

---

## 4.2 步骤2：Flink 作业状态检查

### 检查目标

确认 ODS / DIM / DWD / DWS 主链路作业都在运行，并且没有明显背压和 Checkpoint 故障。

### 必查项

| 检查项 | 通过标准 |
|---|---|
| 作业状态 | `RUNNING` |
| Checkpoint | 连续成功 |
| Backpressure | 无严重背压 |
| End-to-end latency | 在 SLA 范围内 |

### 重点指标

- `job_running_status`
- `checkpoint_success_rate`
- `checkpoint_duration`
- `backpressure_level`
- `processing_latency`

### PromQL 示例

```promql
flink_jobmanager_numRegisteredTaskManagers

max_over_time(flink_taskmanager_job_task_busyTimeMsPerSecond[2m]) / 1000

avg_over_time(flink_jobmanager_job_checkpoint_duration[5m])
```

### 异常处理

| 现象 | 优先处理 |
|---|---|
| 作业失败 | 查日志、恢复作业、确认上次稳定版本 |
| Checkpoint 连续失败 | 优先排查状态后端、Sink、网络 |
| 背压严重 | 查下游 Sink、窗口聚合、Join |

---

## 4.3 步骤3：ODS 层治理检查

### 检查目标

确认源头接入正常，ODS 结果可用且没有明显异常。

### ODS 维度层必查

```sql
SELECT COUNT(*) FROM ods.ods_m_product_latest;
SELECT COUNT(*) FROM ods.ods_m_dim_latest;
SELECT COUNT(*) FROM ods.ods_m_product_alias_latest;
SELECT COUNT(*) FROM ods.ods_m_attribute_latest;
```

### ODS 事实层必查

```sql
SELECT COUNT(*) FROM ods.ods_order_item_cdc;
SELECT COUNT(*) FROM ods.ods_inventory_txn_cdc;
SELECT COUNT(*) FROM ods.ods_kafka_app_click;
SELECT COUNT(*) FROM ods.ods_api_logistics_track;
SELECT COUNT(*) FROM ods.ods_social_mention;
```

### ODS 质量抽查 SQL

#### 主键空值检查

```sql
SELECT *
FROM ods.ods_order_item_cdc
WHERE order_id IS NULL OR line_no IS NULL OR sku_id IS NULL;
```

#### 库存状态合法性检查

```sql
SELECT stock_status, COUNT(*)
FROM ods.ods_inventory_txn_cdc
GROUP BY stock_status;
```

#### JSON 解析失败检查

```sql
SELECT json_parse_status, COUNT(*)
FROM ods.ods_app_click_analyze
GROUP BY json_parse_status;

SELECT json_parse_status, COUNT(*)
FROM ods.ods_api_logistics_track_analyze
GROUP BY json_parse_status;

SELECT json_parse_status, COUNT(*)
FROM ods.ods_social_mention_analyze
GROUP BY json_parse_status;
```

### 通过标准

- ODS 关键表有数据
- 解析失败率可控
- 主键空值无明显异常
- 分区和事件时间没有大面积错乱

---

## 4.4 步骤4：DIM 层治理检查

### 检查目标

确认主数据宽表仍然稳定可用，是下游补维的可信入口。

### 必查 SQL

```sql
SELECT COUNT(*) FROM dim.dim_sku_wide;

SELECT sku_id, spu_code, sku_name, brand_name, season, product_type, color_code, size_code
FROM dim.dim_sku_wide
ORDER BY sku_id
LIMIT 20;
```

### 重点检查项

| 检查项 | 通过标准 |
|---|---|
| `sku_id` 唯一 | 无重复 |
| 关键维度字段 | 无大面积空值 |
| 停用状态 | 可正确识别 |
| 宽表更新时间 | 正常推进 |

### 重复检查 SQL

```sql
SELECT sku_id, COUNT(*)
FROM dim.dim_sku_wide
GROUP BY sku_id
HAVING COUNT(*) > 1;
```

---

## 4.5 步骤5：DWD 层治理检查

### 检查目标

确认明细宽表没有膨胀、补维成功、时间语义稳定。

### 必查对象

- `dwd_order_trade_detail_rt`
- `dwd_inventory_flow_detail_rt`
- `dwd_logistics_track_detail_rt`
- `dwd_user_behavior_detail_rt`
- `dwd_social_mention_detail_rt`
- `dwd_hot_sku_event_detail_rt`

### 推荐 SQL

```sql
SELECT COUNT(*) FROM dwd.dwd_order_trade_detail_rt;
SELECT COUNT(*) FROM dwd.dwd_inventory_flow_detail_rt;
SELECT COUNT(*) FROM dwd.dwd_hot_sku_event_detail_rt;
```

### 主键重复检查

```sql
SELECT order_id, line_no, partition_dt, COUNT(*)
FROM dwd.dwd_order_trade_detail_rt
GROUP BY order_id, line_no, partition_dt
HAVING COUNT(*) > 1;
```

### 补维成功率抽查

```sql
SELECT
  COUNT(*) AS total_cnt,
  SUM(CASE WHEN sku_name IS NOT NULL THEN 1 ELSE 0 END) AS filled_cnt
FROM dwd.dwd_order_trade_detail_rt;
```

### 通过标准

- 关键 DWD 表有数据
- 主键无重复
- 维度字段补齐率达标
- 事件时间字段正常

---

## 4.6 步骤6：DWS 层治理检查

### 检查目标

确认公共粒度正确、指标口径稳定、结果按时产出。

### 必查对象

- `dws_inventory_atp_rt`
- `dws_inventory_dispatch_rt`
- `dws_hot_sku_feature_rt`
- `dws_hot_sku_rank_rt`

### 推荐 SQL

```sql
SELECT COUNT(*) FROM dws.dws_inventory_atp_rt;
SELECT COUNT(*) FROM dws.dws_inventory_dispatch_rt;
SELECT COUNT(*) FROM dws.dws_hot_sku_feature_rt;
SELECT COUNT(*) FROM dws.dws_hot_sku_rank_rt;
```

### 指标抽样核对

```sql
SELECT stat_minute, sku_id, atp_qty, available_qty, locked_qty, in_transit_qty
FROM dws.dws_inventory_atp_rt
ORDER BY stat_minute DESC
LIMIT 20;

SELECT stat_hour, sku_id, hot_score, order_cnt_1h, social_hot_1h
FROM dws.dws_hot_sku_feature_rt
ORDER BY stat_hour DESC, hot_score DESC
LIMIT 20;
```

### 通过标准

- 四张 DWS 表均按时更新
- 核心指标无明显异常波动
- 与 DWD 抽样结果可基本核对

---

## 4.7 步骤7：ADS 层与消费侧检查

### 检查目标

确认结果集刷新稳定，对外消费可用。

### 必查对象

- `ads_inventory_dispatch_dashboard`
- `ads_inventory_dispatch_api`
- `ads_hot_sku_dashboard`
- `ads_hot_sku_reorder_api`

### 推荐 SQL

```sql
SELECT COUNT(*) FROM ads.ads_inventory_dispatch_dashboard;
SELECT COUNT(*) FROM ads.ads_inventory_dispatch_api;
SELECT COUNT(*) FROM ads.ads_hot_sku_dashboard;
SELECT COUNT(*) FROM ads.ads_hot_sku_reorder_api;
```

### 更新时间检查

```sql
SELECT MAX(update_time) FROM ads.ads_inventory_dispatch_dashboard;
SELECT MAX(update_time) FROM ads.ads_hot_sku_dashboard;
```

### 结果抽样检查

```sql
SELECT stat_minute, sku_id, shortage_risk_flag, dispatch_priority_score, suggest_action
FROM ads.ads_inventory_dispatch_api
ORDER BY stat_minute DESC, dispatch_priority_score DESC
LIMIT 20;

SELECT stat_hour, sku_id, hot_score, rank_no, reorder_suggest_flag, reorder_level
FROM ads.ads_hot_sku_reorder_api
ORDER BY stat_hour DESC, rank_no
LIMIT 20;
```

### 通过标准

- ADS 表正常刷新
- 看板结果和接口结果基本一致
- 关键解释字段完整

---

## 五、每日异常时排查手册

## 5.1 ADS 没数据

按顺序排查：

1. ADS 刷新 SQL 是否执行
2. DWS 是否有结果
3. DWD 是否有数据
4. ODS 是否已接入
5. Flink / Doris 是否异常

## 5.2 DWS 延迟高

按顺序排查：

1. Flink 窗口作业状态
2. Checkpoint 是否失败
3. Backpressure 是否升高
4. Doris Sink 是否正常写入
5. 上游 DWD 是否有数据

## 5.3 DWD 结果异常

按顺序排查：

1. DIM 是否正常
2. ODS 事实是否有数据
3. Join 字段类型是否一致
4. 维度补齐率是否下降
5. 是否存在重复膨胀

## 5.4 ODS 异常

按顺序排查：

1. Kafka / MySQL CDC 是否正常
2. 源头数据是否真的来了
3. Flink Source 是否报错
4. 解析失败率是否升高
5. 分区路由是否异常

---

## 六、每周运维操作清单

### 6.1 存储与容量检查

| 检查项 | 关注点 |
|---|---|
| HDFS 容量 | 是否接近阈值 |
| Paimon Snapshot | 是否增长异常 |
| 小文件数量 | 是否需要 Compact |
| Doris 存储 | 热点表是否膨胀 |

### 6.2 物化视图检查

```sql
SHOW ALTER TABLE MATERIALIZED VIEW;
```

重点关注：

- MV 是否刷新成功
- MV 是否长时间未刷新
- MV 是否需要清理

### 6.3 文档一致性检查

每周至少确认一次：

- 数据字典是否与当前实际表结构一致
- 指标口径是否发生变更
- 新增字段是否同步到文档

---

## 七、每月治理复盘清单

### 7.1 治理规则复盘

检查：

- 规则覆盖率是否足够
- 是否仍有高频问题未纳入规则
- 告警是否过多噪音

### 7.2 问题闭环复盘

建议维护问题台账：

| 问题 | 层级 | 首次发现时间 | 责任人 | 当前状态 |
|---|---|---|---|---|
|  |  |  |  |  |

### 7.3 业务口径复盘

重点关注：

- ODS / DIM 字段是否有新增业务需求
- DWS / ADS 指标解释是否发生变化
- 是否需要调整看板字段或接口字段

---

## 八、发布前后验证手册

## 8.1 发布前检查

### 必做项

- 确认变更范围
- 确认是否涉及：
  - 主键
  - 粒度
  - 分区
  - 口径
  - 结果表字段
- 确认回滚方案已准备
- 确认验证 SQL 已准备

### 发布前检查表

| 检查项 | 是否完成 |
|---|---|
| 变更影响分析 |  |
| 业务确认 |  |
| 测试环境验证 |  |
| 回滚方案确认 |  |
| 发布窗口确认 |  |

## 8.2 发布后重点检查

发布后 30 分钟内建议重点检查：

1. 作业是否正常运行
2. Checkpoint 是否连续成功
3. 目标表是否持续出数
4. 结果是否明显偏离预期
5. 消费层是否报错

---

## 九、回滚与补数操作建议

## 9.1 回滚操作原则

- 回滚优先回到上一个稳定版本
- 不在故障未定位清楚时做多次叠加发布
- 回滚后优先看主链路恢复情况

## 9.2 补数操作原则

- 补数优先从最早可控层开始
- ODS 问题优先 ODS 重放
- DWS / ADS 指标问题优先上游重算

### 补数记录模板

| 项目 | 内容 |
|---|---|
| 补数时间 |  |
| 补数层级 |  |
| 补数对象 |  |
| 影响窗口 |  |
| 原因 |  |
| 执行人 |  |
| 验证结果 |  |

---

## 十、推荐告警值守方式

### 10.1 值守顺序

1. 先看 P1 / P0
2. 再看是否影响主链路
3. 再看是否影响 ADS 消费
4. 最后再处理 P2 / P3 趋势问题

### 10.2 推荐告警群分类

| 群组 | 关注内容 |
|---|---|
| 数据开发群 | 作业失败、数据异常、补维异常 |
| 运维群 | 集群、资源、容量、服务存活 |
| 业务接口群 | ADS 延迟、接口 SLA |

---

## 十一、建议的运维记录模板

### 11.1 每日记录

| 日期 | 作业状态 | 质量检查 | ADS 刷新 | 异常说明 | 处理结果 |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

### 11.2 发布记录

| 日期 | 变更内容 | 发布人 | 回滚点 | 发布后结果 |
|---|---|---|---|---|
|  |  |  |  |  |

### 11.3 异常记录

| 时间 | 层级 | 问题现象 | 根因 | 处理方式 | 是否闭环 |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

---

## 十二、通过标准

如果以下条件同时满足，则可认为当前日常治理与监控运维执行有效：

1. 基础服务全部可用
2. Flink 主链路作业稳定运行
3. ODS / DIM / DWD / DWS / ADS 均按时产出
4. 关键质量规则无明显异常
5. ADS 结果可正常服务看板与接口
6. 异常告警可被及时发现并处理

---

## 十三、后续建议

这份手册可以作为你后续定期执行的主操作手册。  
后续如果要继续升级，我建议再补两类文档：

1. `实时湖仓治理规则清单.md`
2. `实时湖仓发布回滚手册.md`

这样你手上就会形成一整套更完整的生产级治理运维包。

