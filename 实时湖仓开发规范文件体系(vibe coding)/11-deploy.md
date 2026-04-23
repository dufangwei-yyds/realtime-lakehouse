# 发布与回滚规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Flink 作业发布、配置变更、回滚操作规范  
> 适用范围：开发环境/测试环境/生产环境作业发布、配置变更、故障回滚

---

## 一、发布前准备

### 1.1 变更范围评估

| 检查项 | 要求 | 说明 |
|-------|------|------|
| 变更类型 | 明确 | 新增作业/配置变更/表结构变更 |
| 影响范围 | 评估 | 上游/下游作业影响 |
| 数据影响 | 评估 | 是否涉及历史数据重算 |
| 回滚方案 | 准备 | 明确的回滚步骤 |

### 1.2 发布前检查清单

- [ ] 变更范围已明确
- [ ] 测试环境验证通过
- [ ] 回滚方案已准备
- [ ] 验证 SQL 已准备
- [ ] 监控告警已配置
- [ ] 通知相关方

---

## 二、发布流程

### 2.1 Flink SQL 作业发布

```bash
# 1. 登录 Flink SQL Client
flink run -t sql-client -d /path/to/job.sql

# 2. 或者通过 SQL Gateway 提交
curl -X POST http://flink-gateway:8083/v1/sql/submissions \
  -H "Content-Type: application/json" \
  -d @job-submission.json
```

### 2.2 DataStream API 作业发布

```bash
# 1. 打包作业
mvn clean package -DskipTests

# 2. 提交作业
flink run \
  --target kubernetes-application \
  --parallelism 4 \
  /path/to/flink-job.jar

# 3. 或者通过 Flink Dashboard 提交
```

### 2.3 Paimon 表变更

```sql
-- 1. 添加字段（Schema Evolution）
ALTER TABLE ods_order_item_cdc ADD COLUMN new_field STRING;

-- 2. 添加分区
ALTER TABLE ods_kafka_app_click ADD PARTITION (_partition_dt='20251231');

-- 3. 变更后验证
SELECT * FROM ods_order_item_cdc WHERE _partition_dt = '20251224' LIMIT 10;
```

---

## 三、发布后验证

### 3.1 验证时机

| 时间点 | 验证内容 |
|-------|---------|
| 发布后 5 分钟 | 作业启动状态 |
| 发布后 15 分钟 | 数据延迟 |
| 发布后 30 分钟 | 数据量、数据质量 |
| 发布后 2 小时 | 业务指标 |

### 3.2 验证检查清单

- [ ] 作业运行状态正常
- [ ] 数据延迟在阈值内
- [ ] 数据量正常
- [ ] 数据质量正常
- [ ] 监控指标正常

---

## 四、回滚操作

### 4.1 回滚触发条件

| 条件 | 说明 | 处理方式 |
|-----|------|---------|
| 作业启动失败 | 无法正常运行 | 回滚 |
| 数据质量异常 | 主键重复率 > 5% | 回滚 |
| 数据量异常 | 偏离预期 > 50% | 评估后决定 |
| 业务指标异常 | 指标计算错误 | 回滚 |

### 4.2 Flink 作业回滚

```bash
# 1. 停止作业
flink cancel -s /path/to/savepoint job_id

# 2. 从上一个 Savepoint 恢复
flink run -d -s /path/to/savepoint /path/to/old-job.jar

# 3. 或者从指定版本恢复
flink run -d -s hdfs:///flink/checkpoints/job_id/chk-xxx /path/to/old-job.jar
```

### 4.3 配置变更回滚

```bash
# 1. 修改回配置文件
# 修改 application.yml 或作业配置

# 2. 重启作业
flink restart job_id

# 3. 或者从 Savepoint 重启
flink run -d -s /path/to/savepoint -r /path/to/job.jar
```

### 4.4 Paimon 表回滚

?? **注意：Paimon 表数据不支持直接回滚，只能通过以下方式处理：**

```sql
-- 方案1：删除异常分区
CALL sys.delete_partitions('paimon_catalog.ods.ods_order_cdc', 'dt=20251224');

-- 方案2：使用历史快照恢复
-- 通过 Flink 作业重算异常分区数据
```

---

## 五、环境配置规范

### 5.1 环境类型

| 环境 | 用途 | 配置要求 |
|-----|------|---------|
| SIT | 开发测试 | 单节点、最小配置 |
| UAT | 联调测试 | 生产环境配置 |
| PROD | 生产环境 | 高可用配置 |

### 5.2 配置差异管理

```sql
-- 方式1：环境变量
SET 'execution.runtime-mode' = '${RUNTIME_MODE}';
SET 'state.backend' = '${STATE_BACKEND}';

-- 方式2：配置文件
-- application-sit.yml
-- application-uat.yml
-- application-prod.yml
```

---

## 六、发布文档规范

### 6.1 发布记录要求

| 字段 | 说明 |
|-----|------|
| 发布编号 | 唯一编号 |
| 发布时间 | 精确到分钟 |
| 发布人 | 执行发布的人员 |
| 变更类型 | 新增/修改/删除 |
| 变更内容 | 具体变更描述 |
| 影响范围 | 上游/下游影响 |
| 验证结果 | 通过/不通过 |
| 回滚记录 | 如有回滚，记录原因和过程 |

### 6.2 发布记录模板

```markdown
# 发布记录

## 发布编号
REL-20251224-001

## 基本信息
- 发布时间：2025-12-24 14:30:00
- 发布人：张三
- 作业名称：job_order_dwd_sit

## 变更内容
- 变更类型：配置变更
- 变更内容：修改水位延迟从 5 分钟调整为 3 分钟
- 变更原因：优化实时性

## 影响范围
- 上游影响：无
- 下游影响：无

## 验证结果
- [x] 作业运行正常
- [x] 数据延迟 < 3 分钟
- [x] 数据量正常
- [x] 数据质量正常

## 回滚记录
- 无
```

---

## 七、发布自检清单

### 7.1 发布前自检

- [ ] 变更范围已明确
- [ ] 测试环境验证通过
- [ ] 回滚方案已准备
- [ ] 验证 SQL 已准备
- [ ] 监控告警已配置

### 7.2 发布后自检

- [ ] 作业运行状态正常
- [ ] 数据延迟在阈值内
- [ ] 数据量正常
- [ ] 数据质量正常

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 06-flink-sql.md | Flink SQL 开发规范 |
| 07-datastream.md | DataStream API 开发规范 |
| 08-paimon.md | Paimon 存储规范 |
| 10-quality.md | 数据质量与验证规范 |

---

> 本文档定义了发布与回滚操作规范，是作业发布和故障处理的核心参考。
