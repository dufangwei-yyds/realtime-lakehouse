# Claude Code 实时湖仓开发规范加载配置

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：Claude Code 在 Cursor 中进行实时湖仓开发时的规范加载配置  
> 适用范围：所有使用 Cursor + Claude Code 进行实时湖仓开发任务的场景

---

## 一、规范文件加载机制

### 1.1 自动加载规则

当在 `d:/workspace/realtime-lakehouse` 目录下启动 Cursor 会话时，Claude Code 会自动读取本目录下的 `CLAUDE.md` 文件。根据任务类型，加载对应的规范子集：

| 任务类型 | 必须加载的规范文件 | 辅助加载的规范文件 |
|---------|-------------------|-------------------|
| 新建 Flink SQL 作业 | 00-meta.md、03-layer-modeling.md、06-flink-sql.md | 02-naming.md、04-field.md |
| 新建 DataStream API 作业 | 00-meta.md、07-datastream.md | 02-naming.md、06-flink-sql.md |
| Paimon 表设计与变更 | 00-meta.md、08-paimon.md | 03-layer-modeling.md、04-field.md |
| Doris ADS 层开发 | 00-meta.md、09-doris.md | 02-naming.md、03-layer-modeling.md |
| 数据质量检查与验证 | 00-meta.md、10-quality.md | 06-flink-sql.md、08-paimon.md |
| 发布与回滚操作 | 00-meta.md、11-deploy.md | 10-quality.md |
| 架构设计与评审 | 00-meta.md、01-tech-stack.md | 03-layer-modeling.md |

### 1.2 规范文件目录结构

```
实时湖仓开发规范文件体系/
├── CLAUDE.md                        # 本文件 - 规范加载配置
├── 00-meta.md                       # AI 行为总纲（所有任务必须加载）
├── 01-tech-stack.md                 # 技术栈与架构规范
├── 02-naming.md                     # 命名规范
├── 03-layer-modeling.md              # 分层建模规范
├── 04-field.md                      # 字段与数据类型规范
├── 05-watermark-partition.md         # 水位与分区规范
├── 06-flink-sql.md                  # Flink SQL 开发规范
├── 07-datastream.md                 # DataStream API 开发规范
├── 08-paimon.md                     # Paimon 存储规范
├── 09-doris.md                      # Doris 开发规范
├── 10-quality.md                    # 数据质量与验证规范
├── 11-deploy.md                     # 发布与回滚规范
├── 12-dos-donts.md                  # 明确禁止项
└── templates/                        # 规范文件模板
    ├── flink-sql-template.md         # Flink SQL 作业模板
    └── paimon-table-template.md      # Paimon 表设计模板
```

---

## 二、AI 生成原则

### 2.1 核心原则

在执行任何实时湖仓开发任务前，Claude Code 必须遵循以下原则：

1. **先查文档，后生成**
   - 所有 SQL 作业生成前，必须先查阅 00-meta.md 和对应的分层建模规范
   - 所有表结构设计前，必须先查阅 02-naming.md 和 04-field.md
   - 所有配置参数，必须在 01-tech-stack.md 中确认是否推荐使用

2. **未定义则禁止**
   - 未在 02-naming.md 中定义的命名规则：禁止使用
   - 未在 04-field.md 中定义的字段类型：禁止使用
   - 未在 06-flink-sql.md 中列出的作业结构模式：禁止自行创造
   - 所有时间字段类型必须使用 TIMESTAMP(3)，不接受 TIMESTAMP(0) 或 DATETIME

3. **分层边界必须遵守**
   - ODS 层：只做贴源、轻度清洗、元数据注入，不做业务聚合
   - DIM 层：统一维度口径，支持 append/history/latest 三种形态
   - DWD 层：以事实过程为核心，明细打宽，维度关联
   - DWS 层：先定义公共粒度，再定义指标
   - ADS 层：面向消费对象设计，不重复底层公共指标

4. **幂等与去重必须处理**
   - 所有事实表必须考虑重复数据处理
   - Kafka Source 必须声明 watermark 策略
   - Paimon 主键表必须明确主键字段

### 2.2 符号约定

| 符号 | 含义 | 说明 |
|-----|------|------|
| #   | 强制要求 | 必须在生成结果中体现 |
| !   | 严格禁止 | 不得出现在任何生成结果中 |
| $   | 最佳实践 | 推荐遵守，可根据场景调整 |
| !!  | 风险提示 | 需要特别注意 |

---

## 三、生成任务自检清单

### 3.1 Flink SQL 作业自检

生成 Flink SQL 作业后，必须进行以下自检：

- [ ] 表名符合 02-naming.md 中的命名规范
- [ ] 所有时间字段使用 TIMESTAMP(3)
- [ ] 金额和数量字段使用 DECIMAL
- [ ] Kafka Source 已声明 WATERMARK
- [ ] 写入 Paimon 的 INSERT 语句包含必要的元数据字段（_event_time, _ingest_time, _partition_dt）
- [ ] 已在 06-flink-sql.md 中确认作业类型（ODS/DIM/DWD/DWS/ADS）
- [ ] 复杂 JOIN 已明确 JOIN 键和时间语义
- [ ] 测试数据模拟脚本已准备

### 3.2 Paimon 表设计自检

生成 Paimon 表结构后，必须进行以下自检：

- [ ] 表名符合分层命名规范
- [ ] 主键字段已在文档中明确记录
- [ ] 分区字段与事件时间语义一致
- [ ] Merge Engine 选择正确（deduplicate/partial-update/aggregation）
- [ ] Bucket 配置合理
- [ ] 已同步更新到数据字典

### 3.3 Doris ADS 层作业自检

生成 Doris 相关作业后，必须进行以下自检：

- [ ] ADS 表面向消费对象设计
- [ ] 物化视图刷新策略已明确
- [ ] 外部表连接 Paimon 的语法正确
- [ ] 单 BE 测试环境已指定 replication_num=1

---

## 四、常用快捷查询

### 4.1 分层命名速查

```
ODS 层：
- CDC 源表：cdc_{table}
- 维度追加表：ods_{table}_append
- 维度历史表：ods_{table}_history
- 维度最新镜像表：ods_{table}_latest
- 事件解析表：ods_{domain}_{event}_analyze

DIM 层：
- 宽表：dim_{domain}_wide

DWD 层：
- 明细表：dwd_{domain}_detail_rt

DWS 层：
- 汇总表：dws_{theme}_{grain}_rt

ADS 层：
- 结果表：ads_{theme}_{consumer}
```

### 4.2 字段类型速查

| 语义 | 类型 | 示例 |
|-----|------|------|
| 金额 | DECIMAL(18,2) | sale_amount DECIMAL(18,2) |
| 数量 | DECIMAL(12,2) | qty DECIMAL(12,2) |
| 时间 | TIMESTAMP(3) | order_time TIMESTAMP(3) |
| 主键 | STRING | sku_id STRING |
| 状态 | STRING | order_status STRING |
| 分区日期 | STRING | _partition_dt STRING |

### 4.3 元数据字段速查

| 字段 | 含义 | 必须注入的层级 |
|-----|------|--------------|
| _event_time | 业务事件时间 | ODS/DWD/DWS |
| _ingest_time | 入湖时间 | ODS |
| _partition_dt | 日期分区 | ODS/DWD/DWS |
| _partition_hh | 小时分区 | ODS（行为流） |
| _kafka_offset | Kafka 偏移量 | ODS（Kafka 接入） |
| _kafka_partition | Kafka 分区号 | ODS（Kafka 接入） |

---

## 五、规范更新与版本管理

### 5.1 规范版本说明

| 版本 | 日期 | 变更说明 |
|-----|------|---------|
| v1.0 | 2026-04 | 初版发布 |

### 5.2 规范更新触发条件

以下情况发生时，必须同步更新本规范文件体系：

1. 新增数据源接入
2. 新增业务主题或指标
3. 分层边界或命名规则变更
4. 技术栈组件升级
5. 数据质量规则变更
6. 发布回滚流程变更

### 5.3 规范同步要求

规范变更必须同步更新：

1. 对应的规范文档
2. 数据字典
3. 相关实现方案文档
4. 测试用例

---

## 六、紧急情况处理

### 6.1 规范冲突处理

当业务需求与规范冲突时：

1. 评估是否必须突破规范
2. 如必须突破，需在 PR/代码中明确说明原因
3. 事后补入 12-dos-donts.md 的豁免条款

### 6.2 规范未覆盖场景

当遇到规范未覆盖的场景时：

1. 先查阅所有相关规范文档
2. 参照 01-tech-stack.md 中的技术选型原则决策
3. 生成结果后主动说明规范未覆盖的部分
4. 事后补充规范

---

## 七、参考文档索引

| 文档名称 | 位置 | 用途 |
|---------|------|------|
| 实时湖仓开发规范 | 方案实现/实时湖仓开发规范.md | 核心开发规范参考 |
| 技术选型报告 | 方案实现/技术选型报告.md | 技术栈决策参考 |
| 数据字典 | 方案实现/实时湖仓数据字典.md | 表结构与字段定义 |
| 业务指标体系清单 | 方案实现/实时湖仓业务指标体系清单.md | 指标口径定义 |
| 发布回滚手册 | 数据治理与监控运维/实时湖仓发布回滚手册.md | 发布流程参考 |

---

> 本配置文件遵循"先查文档，后生成"的核心原则，确保 Claude Code 在执行实时湖仓开发任务时严格遵守企业级开发规范。
