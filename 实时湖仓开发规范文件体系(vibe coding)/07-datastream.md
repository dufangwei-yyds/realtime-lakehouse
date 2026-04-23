# DataStream API 开发规范

> 项目名称：服装行业实时湖仓构建项目一期  
> 文档定位：DataStream API 作业开发规范、使用场景、代码组织  
> 适用范围：复杂状态管理作业、SCD2 处理作业、定制指标输出作业

---

## 一、DataStream API 适用场景

### 1.1 推荐使用场景

| 场景 | 适用原因 | 说明 |
|-----|---------|------|
| SCD2 维度处理 | 需要强状态控制 | 保留维度历史变化 |
| 复杂事件处理 | 需要自定义处理逻辑 | 非标准 CEP 场景 |
| 状态 TTL 管理 | 需要精细控制状态生命周期 | 大状态场景优化 |
| 定制指标输出 | 需要非标准聚合指标 | 自定义业务指标 |
| 多流关联 | 复杂 CoProcessFunction | 多流协调处理 |

### 1.2 不适用场景

? **以下场景应使用 Flink SQL**

| 场景 | 原因 |
|-----|------|
| 标准 ETL 清洗 | Flink SQL 更简洁 |
| 简单窗口聚合 | Flink SQL 窗口函数更完善 |
| 维度关联 | Flink SQL 语法更直观 |
| 基础 JOIN | Flink SQL 支持更好 |

---

## 二、项目结构规范

### 2.1 目录结构

```
flink-jobs/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/lakehouse/
│       │       ├── ods/
│       │       │   ├── ProductScd2Job.java
│       │       │   └── InventoryScd2Job.java
│       │       ├── dim/
│       │       │   └── DimWideJob.java
│       │       └── util/
│       │           ├── MetricsUtil.java
│       │           └── StateUtil.java
│       └── resources/
│           └── application.yml
├── pom.xml
└── README.md
```

### 2.2 作业类结构

```java
package com.lakehouse.ods;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;

/**
 * ODS 层 SCD2 维度处理作业
 * 
 * 功能说明：
 * - 处理商品维度 SCD2 历史变化
 * - 保留历史快照，支持时间旅行查询
 * 
 * 数据流向：
 * - Kafka (CDC 变更流) -> DataStream -> Paimon SCD2 表
 * 
 * 创建时间：2026-04-13
 */
public class ProductScd2Job {

    public static void main(String[] args) throws Exception {
        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env);
        
        // 2. 数据源
        DataStream<String> kafkaStream = createKafkaSource(env);
        
        // 3. 业务逻辑处理
        DataStream<String> processedStream = kafkaStream
            .process(new Scd2ProcessFunction())
            .name("SCD2 Process");
        
        // 4. 输出到 Paimon
        writeToPaimon(processedStream);
        
        // 5. 执行
        env.execute("job_product_scd2");
    }
    
    /**
     * 环境配置
     * 
     * 说明：配置检查点、状态后端等
     */
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        // 检查点配置（必须）
        env.enableCheckpointing(10_000);  // 10 秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // 状态后端（生产环境推荐 rocksdb）
        // env.setStateBackend(new RocksDBStateBackend("hdfs:///flink/checkpoints"));
        
        // 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestartStrategy(
            3,  // 最大失败次数
            org.apache.flink.api.common.time.Time.minutes(5),  // 时间窗口
            org.apache.flink.api.common.time.Time.seconds(30)   // 延迟
        ));
    }
}
```

---

## 三、状态管理规范

### 3.1 状态类型选择

| 状态类型 | 说明 | 适用场景 |
|---------|------|---------|
| ValueState | 单值状态 | 简单计数、标志位 |
| ListState | 列表状态 | 收集多条记录 |
| MapState | 键值状态 | 动态键值映射 |
| AggregatingState | 聚合状态 | 窗口聚合结果 |

### 3.2 状态声明示例

```java
/**
 * SCD2 状态管理
 * 
 * 说明：使用 MapState 存储维度的历史变化
 * - key: sku_id
 * - value: List<Scd2Record> 历史快照列表
 */
public class Scd2ProcessFunction extends ProcessFunction<String, String> {

    // 维度历史状态
    private MapState<String, List<Scd2Record>> dimensionHistoryState;
    
    // 当前版本状态
    private ValueState<String> currentVersionState;
    
    @Override
    public void open(Configuration parameters) {
        // 声明 MapState
        MapStateDescriptor<String, List<Scd2Record>> historyDesc = 
            new MapStateDescriptor<>(
                "dimension_history",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<List<Scd2Record>>() {})
            );
        dimensionHistoryState = getRuntimeContext().getMapState(historyDesc);
        
        // 声明 ValueState
        ValueStateDescriptor<String> versionDesc = 
            new ValueStateDescriptor<>("current_version", TypeInformation.of(String.class));
        currentVersionState = getRuntimeContext().getState(versionDesc);
    }
    
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        // 解析输入
        Scd2Record record = parseRecord(value);
        
        // 获取历史
        List<Scd2Record> history = dimensionHistoryState.get(record.getSkuId());
        if (history == null) {
            history = new ArrayList<>();
        }
        
        // 添加新记录
        history.add(record);
        
        // 更新状态
        dimensionHistoryState.put(record.getSkuId(), history);
        currentVersionState.update(record.getVersion());
        
        // 输出 SCD2 记录
        out.collect(serializeRecord(record));
    }
}
```

### 3.3 状态 TTL 配置

?? **重要：必须配置状态 TTL**

```java
// 状态 TTL 配置
MapStateDescriptor<String, List<Scd2Record>> historyDesc = 
    new MapStateDescriptor<>(...);

// 状态过期时间：7 天
StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofDays(7))
    // 更新类型：读写都更新
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    // 过期策略：定期清理
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();

historyDesc.enableTimeToLive(ttlConfig);
```

---

## 四、指标监控规范

### 4.1 业务指标定义

```java
/**
 * 业务指标监控
 * 
 * 说明：自定义业务指标必须接入监控
 */
public class Scd2ProcessFunction extends ProcessFunction<String, String> {

    // 指标组
    private Counter inputCounter;        // 输入记录数
    private Counter outputCounter;        // 输出记录数
    private Counter errorCounter;         // 错误记录数
    private Histogram recordLatency;      // 处理延迟

    @Override
    public void open(Configuration parameters) {
        // 初始化指标
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        
        inputCounter = metricGroup.counter("scd2_input_count");
        outputCounter = metricGroup.counter("scd2_output_count");
        errorCounter = metricGroup.counter("scd2_error_count");
        recordLatency = metricGroup.histogram("scd2_latency", 
            new DescriptiveStatisticsHistogram(1000));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        long startTime = System.currentTimeMillis();
        
        try {
            inputCounter.inc();
            
            // 业务逻辑
            String result = process(value);
            
            outputCounter.inc();
            out.collect(result);
            
        } catch (Exception e) {
            errorCounter.inc();
            // 错误日志
            LOG.error("Process error: {}", value, e);
        } finally {
            // 记录延迟
            long latency = System.currentTimeMillis() - startTime;
            recordLatency.update(latency);
        }
    }
}
```

### 4.2 关键监控指标

| 指标名 | 类型 | 说明 | 告警阈值 |
|-------|------|------|---------|
| input_count | Counter | 输入记录数 | - |
| output_count | Counter | 输出记录数 | - |
| error_count | Counter | 错误记录数 | > 0 |
| processing_latency | Histogram | 处理延迟 | P99 > 1s |
| state_size | Gauge | 状态大小 | > 10GB |
| checkpoint_duration | Histogram | 检查点耗时 | > 5min |

---

## 五、代码注释规范

### 5.1 类注释要求

```java
/**
 * ODS 层 SCD2 维度处理作业
 * 
 * 功能说明：
 * - 处理商品维度 SCD2 历史变化
 * - 保留历史快照，支持时间旅行查询
 * 
 * 数据流向：
 * - Kafka (CDC 变更流) -> DataStream -> Paimon SCD2 表
 * 
 * 状态说明：
 * - dimension_history: 存储维度历史变化
 * - current_version: 存储当前版本号
 * 
 * 性能指标：
 * - 输入记录数: scd2_input_count
 * - 输出记录数: scd2_output_count
 * - 处理延迟: scd2_latency
 * 
 * 创建时间：2026-04-13
 * 创建人：Claude Code AI
 */
public class ProductScd2Job {
    // ...
}
```

### 5.2 方法注释要求

```java
/**
 * 状态初始化
 * 
 * 说明：声明状态描述符，配置 TTL
 * 
 * @param parameters Flink 配置参数
 */
@Override
public void open(Configuration parameters) {
    // ...
}
```

---

## 六、错误处理规范

### 6.1 异常处理原则

| 原则 | 说明 |
|-----|------|
| 不丢失数据 | 异常数据记录日志，不中断作业 |
| 可追溯 | 异常信息包含完整上下文 |
| 可恢复 | 异常后作业可自动恢复 |

### 6.2 异常处理示例

```java
@Override
public void processElement(String value, Context ctx, Collector<String> out) {
    try {
        // 解析数据
        Scd2Record record = parseRecord(value);
        
        // 业务处理
        String result = processRecord(record);
        
        // 输出结果
        out.collect(result);
        
    } catch (ParseException e) {
        // 解析异常：记录并跳过
        LOG.warn("Parse error, skip record: {}", value, e);
        metrics.errorParseCounter.inc();
        
    } catch (StateException e) {
        // 状态异常：抛出，让 Flink 触发重启
        LOG.error("State error, will trigger restart", e);
        throw e;
        
    } catch (Exception e) {
        // 其他异常：记录，并继续处理
        LOG.error("Process error, skip record: {}", value, e);
        metrics.errorCounter.inc();
    }
}
```

---

## 七、DataStream API 自检清单

### 7.1 项目结构检查

- [ ] 目录结构符合规范
- [ ] 包名命名规范
- [ ] 资源文件配置正确

### 7.2 环境配置检查

- [ ] 检查点配置完整
- [ ] 状态后端配置合理
- [ ] 重启策略配置正确

### 7.3 状态管理检查

- [ ] 状态类型选择正确
- [ ] 状态 TTL 已配置
- [ ] 状态序列化正确

### 7.4 指标监控检查

- [ ] 业务指标已定义
- [ ] 错误指标已定义
- [ ] 延迟指标已定义

### 7.5 代码质量检查

- [ ] 类注释完整
- [ ] 方法注释完整
- [ ] 异常处理完善
- [ ] 日志记录规范

---

## 八、后续文档索引

| 文档 | 定位 |
|-----|------|
| 03-layer-modeling.md | 分层建模规范 |
| 06-flink-sql.md | Flink SQL 开发规范 |
| 08-paimon.md | Paimon 存储规范 |
| 10-quality.md | 数据质量与验证规范 |

---

> 本文档定义了 DataStream API 作业开发规范，是复杂状态管理作业开发的核心参考。
