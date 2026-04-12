# ODS 层数据质量和监控预警方案


## ODS 层数据质量和监控预警方案

### 一、整体设计说明

#### 1.1 业务背景

在实时数仓/实时湖仓中，SCD2（缓慢变化维）是：商品主数据、用户画像、组织/门店/品类维度的绝对核心基础能力。

Flink + Paimon 是目前实时 SCD2 的最优组合之一，但生产问题集中在：


| 问题类型       | 说明                              |
| ---------- | ------------------------------- |
| **正确性不可知** | 无法确认 SCD2 是否"健康"                |
| **膨胀不可见**  | Paimon 表膨胀、Compaction 异常不可见     |
| **事故无预警**  | Checkpoint/Backpressure 事故无提前预警 |


#### 1.2 架构图

```
[CDC 数据源] → [Flink SCD2 Job] → [Paimon Sink]
       │                      │
       │                       └→ Prometheus metrics
       │                             │
       │                             └→ Alertmanager → 钉钉告警
       │
       └→ Grafana Dashboard
```

#### 1.3 设计目标


| 维度      | 目标                             |
| ------- | ------------------------------ |
| **正确性** | 严格 SCD2 语义（行闭合 + 新版本）          |
| **稳定性** | Checkpoint、背压、延迟可观测            |
| **可运维** | 作业级、集群级、存储级指标齐全                |
| **可告警** | Prometheus + Alertmanager 自动预警 |


---

### 二、监控指标设计

#### 2.1 SCD2 作业级自定义业务指标


| 指标                       | 意义      |
| ------------------------ | ------- |
| scd2_input_total         | 输入数据量   |
| scd2_output_total        | 新版本生成量  |
| scd2_nochange_total      | 无变化更新   |
| scd2_close_version_total | 成功闭合版本  |
| scd2_version_jump_total  | 时间/版本异常 |


#### 2.2 Flink 原生系统级指标


| 指标                                             | 意义              |
| ---------------------------------------------- | --------------- |
| flink_jobmanager_numRegisteredTaskManagers     | 集群是否健康          |
| flink_taskmanager_Status_JVM_CPU_Load          | TM 是否在干活        |
| flink_taskmanager_job_task_busyTimeMsPerSecond | 是否背压            |
| flink_jobmanager_job_checkpoint_*              | Checkpoint 是否稳定 |


#### 2.3 Paimon 维表健康度增强指标


| 指标                          | 说明            |
| --------------------------- | ------------- |
| paimon_sink_records_written | 实际写入量         |
| paimon_compaction_duration  | Compaction 耗时 |
| paimon_compaction_pending   | Compaction 积压 |
| paimon_snapshot_count       | Snapshot 增长趋势 |


> **指标获取方案**：Paimon Sink 内置 Metrics（Flink 1.18+）
>
> - `recordsWritten`
> - `compactionDuration`
> - `compactionPending`
> - `snapshotCount`
>
> **前提**：`'metrics.enabled' = 'true'`
>
> **Snapshot 数量趋势告警**：`increase(paimon_snapshot_count[1h]) > 100`（这是"慢性膨胀"的唯一可靠预警方式）

---

### 三、与 Prometheus / Grafana 的对接方式

#### 3.1 Flink → Prometheus 对接

```bash
# 1. 复制 Prometheus 连接器
lib/flink-metrics-prometheus-1.20.3.jar

# 2. 配置 config.yaml
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249-9259

# 3. 启动服务
./bin/start-cluster.sh
./bin/historyserver.sh start

# 4. 验证
curl http://192.168.63.128:9249/metrics
curl http://192.168.63.128:9250/metrics
```

#### 3.2 Prometheus → Flink 抓取

```yaml
# prometheus.yaml
scrape_configs:
  - job_name: "flink"
    static_configs:
      - targets: ["192.168.63.128:9249"]
      - targets: ["192.168.63.128:9250"]
```

#### 3.3 Grafana Dashboard 设计


| Row   | 内容                                                                          |
| ----- | --------------------------------------------------------------------------- |
| Row 1 | **Cluster Overview**：TM 数量、CPU Load、Job Running 数                           |
| Row 2 | **Job Throughput & Latency（核心）**：SCD2 Input/Output rate、Task busyTime、端到端延迟 |
| Row 3 | **Checkpoint（Paimon 必看）**：成功/失败次数、Checkpoint Duration、对齐时间                  |
| Row 4 | **Backpressure（事故高发）**：busyTimeMsPerSecond、吞吐下降趋势                           |
| Row 5 | **Paimon Sink 专用（重点）**：写入量、Compaction Pending、Snapshot 增长                   |


#### 3.4 Grafana Dashboard JSON

```json
{
  "title": "Flink + Paimon SCD2 Production Dashboard",
  "timezone": "browser",
  "schemaVersion": 38,
  "version": 1,
  "panels": [
    {
      "type": "stat",
      "title": "Registered TaskManagers",
      "targets": [
        {
          "expr": "flink_jobmanager_numRegisteredTaskManagers",
          "refId": "A"
        }
      ]
    },
    {
      "type": "time_series",
      "title": "SCD2 Input / Output Rate",
      "targets": [
        {
          "expr": "rate(flink_taskmanager_job_task_operator_scd2_input_total[1m])",
          "legendFormat": "input"
        },
        {
          "expr": "rate(flink_taskmanager_job_task_operator_scd2_output_total[1m])",
          "legendFormat": "output"
        }
      ]
    },
    {
      "type": "time_series",
      "title": "Checkpoint Duration",
      "targets": [
        {
          "expr": "flink_jobmanager_job_checkpoint_duration",
          "legendFormat": "checkpoint"
        }
      ]
    },
    {
      "type": "time_series",
      "title": "Backpressure (busyTime)",
      "targets": [
        {
          "expr": "flink_taskmanager_job_task_busyTimeMsPerSecond",
          "legendFormat": "busyTime"
        }
      ]
    },
    {
      "type": "time_series",
      "title": "Paimon Compaction Pending",
      "targets": [
        {
          "expr": "paimon_compaction_pending",
          "legendFormat": "pending"
        }
      ]
    }
  ]
}
```

#### 3.5 Grafana 中查看 SCD2 核心指标

```promql
-- SCD2 输入速率
rate(flink_taskmanager_job_task_operator_scd2_input_total[1m])

-- 无变化比例
rate(flink_taskmanager_job_task_operator_scd2_nochange_total[5m])
/
rate(flink_taskmanager_job_task_operator_scd2_input_total[5m])
```

#### 3.6 Prometheus 告警规则

```yaml
# flink-paimon-alert-rules.yml
groups:
  - name: flink-paimon-scd2
    rules:

      # Checkpoint 告警
      - alert: FlinkCheckpointFailure
        expr: rate(flink_jobmanager_job_checkpoint_failed[5m]) > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Flink Checkpoint 连续失败"

      - alert: FlinkCheckpointSlow
        expr: flink_jobmanager_job_checkpoint_duration > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Checkpoint 耗时过长"

      # Backpressure 告警
      - alert: FlinkBackpressureHigh
        expr: flink_taskmanager_job_task_busyTimeMsPerSecond > 800
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "作业出现严重背压"

      # 延迟告警（业务 SLA）
      - alert: FlinkHighLatency
        expr: flink_taskmanager_job_task_latency_ms > 5000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "作业处理延迟过高"

      # Paimon Sink 告警
      - alert: PaimonCompactionBacklog
        expr: paimon_compaction_pending > 5
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Paimon Compaction 积压"
```

#### 3.7 生产级 Prometheus 告警规则

```yaml
# ==============================
# Production Prometheus Alert Rules
# Flink + Paimon SCD2
# ==============================

groups:
  - name: flink_scd2_alerts
    interval: 30s
    rules:

      # 1. Checkpoint 告警
      - alert: FlinkCheckpointConsecutiveFailures
        expr: increase(flink_jobmanager_job_checkpoint_failures_total[5m]) >= 3
        for: 1m
        labels:
          severity: critical
          type: checkpoint
        annotations:
          summary: "Flink Job {{ $labels.job }} 连续 Checkpoint 失败"

      - alert: FlinkCheckpointDurationHigh
        expr: avg_over_time(flink_jobmanager_job_checkpoint_duration[5m]) > 60
        for: 1m
        labels:
          severity: warning
          type: checkpoint
        annotations:
          summary: "Flink Job {{ $labels.job }} Checkpoint 耗时过长"

      # 2. Backpressure 告警
      - alert: FlinkTaskBackpressureHigh
        expr: max_over_time(flink_taskmanager_job_task_busyTimeMsPerSecond[2m]) / 1000 > 0.8
        for: 1m
        labels:
          severity: critical
          type: backpressure
        annotations:
          summary: "Flink Job {{ $labels.job }} 任务背压严重"

      # 3. 延迟告警（业务 SLA）
      - alert: Scd2ProcessingLatencyHigh
        expr: scd2_processing_latency_seconds{job~".*"} > 10
        for: 1m
        labels:
          severity: warning
          type: latency
        annotations:
          summary: "SCD2 Job {{ $labels.job }} 延迟过高"

      # 4. Paimon Sink 专属告警
      - alert: PaimonCompactionStalled
        expr: (time() - paimon_sink_last_compaction_timestamp_seconds) > 600
        for: 1m
        labels:
          severity: warning
          type: paimon
        annotations:
          summary: "Paimon Sink {{ $labels.job }} Compaction 长时间未触发"

      - alert: PaimonSnapshotFailures
        expr: increase(paimon_sink_snapshot_failures_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
          type: paimon
        annotations:
          summary: "Paimon Sink {{ $labels.job }} Snapshot 失败"

      - alert: Scd2VersionJumpDetected
        expr: scd2_version_jump_total{job~".*"} > 0
        for: 1m
        labels:
          severity: warning
          type: scd2
        annotations:
          summary: "SCD2 Job {{ $labels.job }} 版本跳跃异常"
```

> **使用说明**：
>
> 1. 将 YAML 文件保存至 `/etc/prometheus/rules/flink_scd2_alerts.yml`
> 2. Prometheus 配置 `prometheus.yml` 中引入：`rule_files: - "rules/flink_scd2_alerts.yml"`
> 3. 重启 Prometheus 或执行 `/-/reload`
> 4. 确保 Alertmanager 对接配置完成（如钉钉 Webhook）

#### 3.8 Alertmanager 对接钉钉

```yaml
receivers:
  - name: 'dingding'
    webhook_configs:
      - url: 'https://oapi.dingtalk.com/robot/send?access_token=XXXX'
```

---

### 四、Scd2ProductJob 的集成点

```java
// 无需额外侵入业务逻辑
stream
    .keyBy(productId)
    .flatMap(new Scd2ProductProcessFunction())
    .sinkTo(paimonSink);
```

---

### 五、这套设计在生产中的真正价值


| 传统问题             | 现在                |
| ---------------- | ----------------- |
| SCD2 是否正常不知道     | Dashboard 秒级可见    |
| Paimon 表无声膨胀     | Compaction 指标提前预警 |
| Checkpoint 事故靠报警 | 预测 + 预防           |
| 运维靠经验            | 运维靠指标             |


---

### 六、升级方向

#### 升级方向 1：SCD2 Metrics & ODS 白皮书


| ODS 一致性维度  | 实时指标                             | 判定信号              | 是否需要触发校验 |
| ---------- | -------------------------------- | ----------------- | -------- |
| 行数一致性      | scd2_input_total vs output_total | output/input 比例异常 | 可能       |
| 主键唯一性      | scd2_version_jump_total          | > 0               | 必须       |
| SCD2 时序正确性 | scd2_close_version_total         | 关闭失败              | 必须       |
| 数据延迟 SLA   | scd2_processing_latency          | > SLA             | 视情况      |
| 存储可见性      | paimon_compaction_pending        | 持续增长              | 先等       |


> **核心设计思想**：不是每个异常都触发校验，而是只有"语义可能被破坏"的指标，才升级为抽样校验

#### 升级方向 2：为 Paimon 增加健康评分


| 维度                 | 规则                                   | 扣分  |
| ------------------ | ------------------------------------ | --- |
| Snapshot 增长        | `increase(snapshot_count[1h]) > 100` | -20 |
| Compaction Pending | > 5 持续 10min                         | -30 |
| 无 Compaction       | 10min 未触发                            | -20 |
| 写入失败               | `snapshot_failures > 0`              | -40 |



| Score | 状态            | 含义    |
| ----- | ------------- | ----- |
| ≥ 90  | **Healthy**   | 可放心对账 |
| 70–90 | **Degraded**  | 延后对账  |
| < 70  | **Unhealthy** | 禁止对账  |


> **重要结论**：只有 Paimon ≥ Healthy，校验结果才"可信"

#### 升级方向 3：告警分级 → 自动化动作


| 告警级别   | 示例                   | 自动动作           |
| ------ | -------------------- | -------------- |
| **P0** | Checkpoint 连续失败      | 自动暂停下游对账       |
| **P1** | SCD2 Version Jump    | 标记表 → 加入抽样校验队列 |
| **P2** | Paimon Compaction 积压 | 延迟校验执行         |
| **P3** | 延迟轻微升高               | 仅记录            |


**推荐的"动作输出"**：写一条 `ods_quality_event` 表/Topic

```json
{
    "table": "ods_m_product_scd2",
    "event": "SCD2_VERSION_JUMP",
    "severity": "P1",
    "action": "SCHEDULE_SAMPLING_CHECK",
    "timestamp": "2026-01-14T20:12:00"
}
```

---
