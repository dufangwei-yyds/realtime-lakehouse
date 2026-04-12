# Fluss PoC 操作验证文档

> 文档定位：`Apache Fluss 0.8.0` 第二阶段最小链路 PoC OOTB 操作手册  
> 适用环境：已部署本地集群版 `Fluss 0.8.0`，并已在 `Flink 1.20.x SQL Client` 中成功创建 `fluss_catalog`  
> 验证目标：在不改动一期主生产链路的前提下，验证 `Log Table`、`PrimaryKey Table`、`Streaming Read` 与 `Lookup Join`

---

## 一、PoC 目标

本轮 PoC 只做最小闭环验证，聚焦两个场景：

- 场景 A：行为流 + 商品状态补全
- 场景 B：库存状态实时表

本轮不做：

- 主生产链路迁移
- 大规模压测
- 多作业复杂编排
- 与一期现网表直接互通

---

## 二、验证范围与通过标准

### 2.1 范围

- 验证 `Fluss Catalog` 可正常使用
- 验证 `Log Table` 的建表、写入、读取
- 验证 `PrimaryKey Table` 的建表、写入、更新、读取
- 验证基于 `PrimaryKey Table` 的 `Lookup Join`
- 验证 streaming read 的基础行为

### 2.2 通过标准

满足以下条件即可视为本轮 PoC 跑通：

1. `Fluss Catalog`、database、table 全部创建成功
2. 行为流测试数据能成功写入 `Log Table`
3. 商品状态与库存状态测试数据能成功写入 `PrimaryKey Table`
4. 场景 A 的 `Lookup Join` 能成功输出富化结果
5. 场景 B 能观察到主键表“最新态”结果
6. 能用 streaming read 正常读取 `Fluss` 表数据

---

## 三、前置检查

在开始前，请先确认：

1. 本地 `Fluss 0.8.0` 已正常启动
2. `Flink 1.20.x` 已正常启动
3. `Flink SQL Client` 中已能执行：

```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);
```

4. 当前 `Fluss 0.8` 对 `Flink 1.20` 是官方支持组合
5. 当前 PoC 建议使用独立 database，不污染其他实验对象

### 3.1 已确认的环境注意事项

在本轮实际验证中，已经确认一个关键环境点：

- 若 `Fluss` 底层使用 `hdfs://` 存储路径
- 即使 `Fluss` 自身插件目录中已经存在 HDFS 相关插件
- `Flink SQL Client` 在执行 `SELECT` / streaming read 时，`Flink` 进程侧仍需要具备对应文件系统支持

实际现象如下：

```text
org.apache.fluss.fs.UnsupportedFileSystemSchemeException:
Could not find a file system implementation for scheme 'hdfs'
```

本次验证中已确认可行的处理方式为：

1. 在 `Fluss` 的 plugin 目录下确认存在 HDFS 相关插件 jar
2. 将 `fluss-fs-hdfs-0.8.0-incubating.jar` 复制到 `Flink` 的 `lib` 目录
3. 重启 `Fluss` 与 `Flink`
4. 再重新执行 streaming read / query

本次已验证通过的结论是：

- 仅 `Fluss plugin` 侧具备 HDFS 支持还不够
- `Flink` 侧也需要能够识别该文件系统实现，否则查询阶段仍会报错

建议后续所有 PoC 环境默认纳入这一检查项。

参考：

- Flink Engine Getting Started: https://fluss.apache.org/docs/engine-flink/getting-started/
- Flink DDL: https://fluss.apache.org/docs/next/engine-flink/ddl/

---

## 四、建议执行顺序

建议严格按以下顺序执行：

1. 创建 catalog 和 database
2. 创建场景 A 的 source / Fluss 表 / sink
3. 执行场景 A 的写入与 lookup join
4. 创建场景 B 的 source / Fluss 表
5. 执行场景 B 的状态写入与读取验证
6. 做 streaming read 与抽样校验
7. 记录结果并输出初步结论

---

## 五、公共准备

### 5.1 创建 Catalog

如你已经创建过，可跳过。

```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = '192.168.63.128:9123'
);

USE CATALOG fluss_catalog;
```

说明：

- `bootstrap.servers` 请替换为你本机实际 `CoordinatorServer` 地址
- 官方文档说明可写一个或多个 Fluss server 地址，默认快速实验一般填本地 `localhost:9123`

### 5.2 创建独立实验库

```sql
CREATE DATABASE IF NOT EXISTS poc_fashion;
USE poc_fashion;
```

---

## 六、场景 A：行为流 + 商品状态补全

### 6.1 目标

验证 3 件事：

1. 行为流写入 `Log Table`
2. 商品状态写入 `PrimaryKey Table`
3. 使用 `Lookup Join` 完成实时富化

### 6.2 建表设计

本场景包含 4 个对象：

- 行为源表：`src_app_click`
- 商品状态源表：`src_sku_status`
- 行为日志表：`fluss_app_click_log`
- 商品状态主键表：`fluss_sku_status_pk`

另补一个富化结果表：

- `fluss_behavior_enriched_log`

### 6.3 创建行为源表

这里采用 `datagen`，方便先跑通链路。  
如果你后面想切成静态样例数据，可再替换。

```sql
CREATE TEMPORARY TABLE src_app_click (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '20',
  'fields.event_id.kind' = 'sequence',
  'fields.event_id.start' = '1',
  'fields.event_id.end' = '5000',
  'fields.user_id.min' = '10001',
  'fields.user_id.max' = '10100',
  'fields.sku_id.min' = '1001',
  'fields.sku_id.max' = '1020',
  'fields.event_type.length' = '8'
);
```

说明：

- 这里建议数据量稍微放大一点，便于观察读取与 lookup 表现
- `event_type` 用 datagen 随机字符串即可，当前不强依赖枚举语义

### 6.4 创建商品状态源表

```sql
CREATE TEMPORARY TABLE src_sku_status (
  sku_id BIGINT,
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT,
  status_time TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.sku_id.min' = '1001',
  'fields.sku_id.max' = '1020',
  'fields.sku_name.length' = '12',
  'fields.brand_name.length' = '6',
  'fields.product_type.length' = '6',
  'fields.available_flag.min' = '0',
  'fields.available_flag.max' = '1'
);
```

### 6.5 创建 Fluss Log Table

```sql
CREATE TABLE fluss_app_click_log (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3)
) WITH (
  'bucket.num' = '4'
);
```

说明：

- 未定义 `PRIMARY KEY`，因此这是 `Log Table`

### 6.6 创建 Fluss PrimaryKey Table

```sql
CREATE TABLE fluss_sku_status_pk (
  sku_id BIGINT,
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT,
  status_time TIMESTAMP(3),
  PRIMARY KEY (sku_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### 6.7 创建富化结果表

```sql
CREATE TABLE fluss_behavior_enriched_log (
  event_id BIGINT,
  user_id BIGINT,
  sku_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  sku_name STRING,
  brand_name STRING,
  product_type STRING,
  available_flag INT
) WITH (
  'bucket.num' = '4'
);
```

说明：

- 这里不再使用 `print` sink
- 改成可查询的 `Fluss Log Table`
- 这样后续可以直接通过 `SELECT` 查看 join 富化结果，更适合 PoC 验证

### 6.8 执行写入

先写行为流：

```sql
INSERT INTO fluss_app_click_log
SELECT *
FROM src_app_click;
```

再写商品状态：

```sql
INSERT INTO fluss_sku_status_pk
SELECT *
FROM src_sku_status;
```

说明：

- 两个 `INSERT` 建议分别在不同 SQL Client 会话或独立提交中运行
- 若只做短时间验证，运行 1 到 3 分钟后可手动停止

### 6.9 验证 streaming read

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_app_click_log;
```

可选：

```sql
SELECT * FROM fluss_sku_status_pk;
```

说明：

- 官方说明默认 streaming read 会先读当前快照，再持续读增量

### 6.10 验证 Lookup Join

```sql
INSERT INTO fluss_behavior_enriched_log
SELECT
  a.event_id,
  a.user_id,
  a.sku_id,
  a.event_type,
  a.event_time,
  b.sku_name,
  b.brand_name,
  b.product_type,
  b.available_flag
FROM (
  SELECT fluss_app_click_log.*, PROCTIME() AS ptime
  FROM fluss_app_click_log
) AS a
LEFT JOIN fluss_sku_status_pk
FOR SYSTEM_TIME AS OF a.ptime AS b
ON a.sku_id = b.sku_id;
```

说明：

- 官方要求 `Lookup Join` 必须基于 `PrimaryKey Table`
- join 条件必须覆盖维表全部主键
- `Fluss` 默认异步 lookup

### 6.11 场景 A 通过标准

- `fluss_app_click_log` 有持续写入
- `fluss_sku_status_pk` 有持续更新
- `fluss_behavior_enriched_log` 能持续写入富化后的结果
- 富化结果中 `sku_name / brand_name / product_type / available_flag` 能正常补全

### 6.12 查询富化结果

先用 streaming 模式观察：

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_behavior_enriched_log;
```

如需抽样检查，也可切 batch 模式：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_behavior_enriched_log
LIMIT 50;
```

说明：

- 当前 `Fluss 0.8.0` 下，富化结果表本质仍按 `Log Table` 方式承载
- 对 `Log Table` 做完整结果查询时，更建议使用 `streaming` 模式
- `batch` 模式更适合 `LIMIT` 抽样预览

---

## 七、场景 B：库存状态实时表

### 7.1 目标

验证 3 件事：

1. `PrimaryKey Table` 是否适合库存状态承载
2. 同主键多次写入后，是否体现“最新态”
3. streaming read 是否可观察状态变化

### 7.2 建表设计

本场景包含 2 个对象：

- 库存状态源表：`src_inventory_state`
- 库存状态主键表：`fluss_inventory_state_pk`

### 7.3 创建库存状态源表

```sql
CREATE TEMPORARY TABLE src_inventory_state (
  sku_id BIGINT,
  store_id BIGINT,
  available_qty INT,
  locked_qty INT,
  in_transit_qty INT,
  update_time TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.sku_id.min' = '1001',
  'fields.sku_id.max' = '1020',
  'fields.store_id.min' = '1',
  'fields.store_id.max' = '5',
  'fields.available_qty.min' = '0',
  'fields.available_qty.max' = '300',
  'fields.locked_qty.min' = '0',
  'fields.locked_qty.max' = '60',
  'fields.in_transit_qty.min' = '0',
  'fields.in_transit_qty.max' = '80'
);
```

### 7.4 创建库存状态主键表

```sql
CREATE TABLE fluss_inventory_state_pk (
  sku_id BIGINT,
  store_id BIGINT,
  available_qty INT,
  locked_qty INT,
  in_transit_qty INT,
  update_time TIMESTAMP(3),
  PRIMARY KEY (sku_id, store_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### 7.5 执行写入

```sql
INSERT INTO fluss_inventory_state_pk
SELECT *
FROM src_inventory_state;
```

### 7.6 验证读取

先看 streaming read：

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_inventory_state_pk;
```

再看 batch 抽样：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_inventory_state_pk
LIMIT 20;
```

### 7.7 观察点

重点观察：

- 同一 `(sku_id, store_id)` 是否不断更新
- 读取结果是否体现“最新状态”
- 状态表写入与读取是否明显比单纯 append 表更贴近业务实时状态语义

### 7.8 场景 B 通过标准

- `fluss_inventory_state_pk` 有稳定写入
- 能读取到库存状态数据
- 同主键下结果体现主键表“保留最新值”的语义

---

## 八、抽样校验 SQL

### 8.1 校验行为日志表

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT COUNT(*) AS cnt
FROM fluss_app_click_log;
```

### 8.2 校验商品状态主键表

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT COUNT(*) AS cnt
FROM fluss_sku_status_pk;
```

### 8.3 校验库存状态主键表

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT COUNT(*) AS cnt
FROM fluss_inventory_state_pk;
```

### 8.4 抽样看库存最新态

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT * FROM fluss_inventory_state_pk;
```

如需在 batch 模式验证，建议优先使用主键点查：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_inventory_state_pk
WHERE sku_id = 1001 AND store_id = 1;
```

### 8.5 抽样看行为富化结果

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT *
FROM fluss_behavior_enriched_log
LIMIT 50;
```

---

## 九、常见问题排查

### 9.1 Catalog 已创建但建表失败

优先检查：

- 当前是否已 `USE CATALOG fluss_catalog`
- 当前是否已 `USE poc_fashion`
- `bucket.num` 是否配置

### 9.2 `SELECT` 或 streaming read 报 `scheme 'hdfs'` 不支持

典型报错：

```text
org.apache.fluss.fs.UnsupportedFileSystemSchemeException:
Could not find a file system implementation for scheme 'hdfs'
```

处理方式：

1. 确认 `Fluss` plugin 目录下已有 HDFS 相关插件
2. 将 `fluss-fs-hdfs-0.8.0-incubating.jar` 放入 `Flink lib` 目录
3. 重启 `Fluss`
4. 重启 `Flink`
5. 再重新执行查询

本轮 PoC 已确认上述方式有效。

### 9.2 `Lookup Join` 报错

优先检查：

- 被 lookup 的表是否是 `PrimaryKey Table`
- join 条件是否覆盖全部主键
- 是否按官方要求使用：

```sql
FOR SYSTEM_TIME AS OF a.ptime
```

### 9.3 读取结果与预期不一致

优先检查：

- 当前是 `streaming` 模式还是 `batch` 模式
- 是否误把 `Log Table` 当成主键表理解
- 是否误把主键表结果理解成 changelog 明细而不是最新态

### 9.4 `SELECT * FROM Fluss 表` 报 batch 查询限制

典型报错：

```text
java.lang.UnsupportedOperationException:
Currently, Fluss only support queries on table with datalake enabled
or point queries on primary key when it's in batch execution mode.
```

处理建议：

1. 查询 `Log Table` 或富化结果表时，优先使用 `streaming` 模式
2. `batch` 模式下若只是预览，请使用 `LIMIT`
3. 查询 `PrimaryKey Table` 时：
   - 看整体结果，优先使用 `streaming`
   - 用 `batch` 时，优先做主键点查
4. 当前 PoC 不建议用 `batch` 模式直接全表扫描 `Fluss` 表

### 9.5 原来使用 `print` sink 时无法 `SELECT`

说明：

- `print` connector 只能作为 sink 使用
- 不能被当成 source 查询

因此本手册已经统一改为：

- 使用 `fluss_behavior_enriched_log` 作为可查询结果表

后续如需查看富化结果，请直接查询：

```sql
SELECT *
FROM fluss_behavior_enriched_log;
```

---

## 十、PoC 结果记录建议

建议记录以下内容：

1. 环境版本
2. Catalog 创建是否成功
3. 场景 A 是否跑通
4. 场景 B 是否跑通
5. `Lookup Join` 是否成功
6. 主要异常与处理方式
7. 初步结论：是否值得继续进入下一阶段

---

## 十一、初步结论判定建议

若以下条件同时满足，可认为 `Fluss` 值得继续进入下一阶段：

- `Log Table` 行为流链路跑通
- `PrimaryKey Table` 库存状态链路跑通
- `Lookup Join` 正常可用
- 团队对其 SQL 使用方式已具备基础认知
- 当前接入复杂度处于可接受范围

---

## 附录：官方参考资料

- Getting Started with Flink: https://fluss.apache.org/docs/engine-flink/getting-started/
- Flink DDL: https://fluss.apache.org/docs/next/engine-flink/ddl/
- Flink Reads: https://fluss.apache.org/docs/engine-flink/reads/
- Flink Writes: https://fluss.apache.org/docs/engine-flink/writes/
- Flink Lookup Joins: https://fluss.apache.org/docs/0.5/engine-flink/lookups/
- Primary Key Table: https://fluss.apache.org/docs/table-design/table-types/pk-table/
