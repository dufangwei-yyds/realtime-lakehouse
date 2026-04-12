package org.dufangwei.flink.scd2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import static org.apache.flink.table.api.DataTypes.*;

public class Scd2ProductJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // === 设置作业并行度 ===
        env.setParallelism(3);

        // === 设置Checkpoint ===
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.63.128:8020/flink/checkpoints");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // === 创建 paimon_catalog & 创建并切换到 paimon_catalog.ods===
        tEnv.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "  'type' = 'paimon',                    \n" +
                "  'warehouse' = 'hdfs://192.168.63.128:8020/paimon/erp_dw' \n" +
                ")");
        tEnv.executeSql("USE paimon_catalog");
        tEnv.executeSql("CREATE DATABASE ID NOT EXISTS paimon_catalog.ods");
        tEnv.executeSql("USE paimon_catalog.ods");

        // === 1. 注册 CDC 源表 ===
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE cdc_m_product (\n" +
                        "  id                STRING,\n" +
                        "  name              STRING,\n" +
                        "  `value`           STRING,\n" +
                        "  m_dim_id          STRING,\n" +
                        "  m_attributesetinstance_id STRING,\n" +
                        "  tag_price         DECIMAL(12,2),\n" +
                        "  guide_price       DECIMAL(12,2),\n" +
                        "  pdt_cost          DECIMAL(12,2),\n" +
                        "  market_date       DATE,\n" +
                        "  created_at        TIMESTAMP(3),\n" +
                        "  updated_at        TIMESTAMP(3),\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = '192.168.0.104',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'Dfw920130Q520,',\n" +
                        "  'server-time-zone' = 'Asia/Shanghai',\n" +
                        "  'database-name' = 'erp_db',\n" +
                        "  'table-name' = 'M_PRODUCT',\n" +
                        "  'scan.startup.mode' = 'initial',\n" +
                        "  'debezium.log.mining.strategy' = 'online_catalog'\n" +
                        ")"
        );

        // === 2. 注册 Paimon SCD2 目标表 ===
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS ods_m_product_history_v3 (\n" +
                        "  id                STRING,\n" +
                        "  row_data          ROW<\n" +
                        "    name     STRING,\n" +
                        "    `value`  STRING,\n" +
                        "    m_dim_id STRING,\n" +
                        "    m_attributesetinstance_id STRING,\n" +
                        "    tag_price DECIMAL(12,2),\n" +
                        "    guide_price DECIMAL(12,2),\n" +
                        "    pdt_cost DECIMAL(12,2),\n" +
                        "    market_date DATE,\n" +
                        "    created_at TIMESTAMP(3),\n" +
                        "    updated_at TIMESTAMP(3)\n" +
                        "  >,\n" +
                        "  start_time        TIMESTAMP(3),\n" +
                        "  end_time          TIMESTAMP(3),\n" +
                        "  is_current        BOOLEAN,\n" +
                        "  version           BIGINT,\n" +
                        "  PRIMARY KEY (id, start_time) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'paimon',\n" +
                        "  'merge-engine' = 'aggregation',\n" +
                        "  'fields.row_data.aggregate-function' = 'last_value',\n" +
                        "  'fields.row_data.ignore-retract' = 'true',\n" +
                        "  'fields.end_time.aggregate-function' = 'last_value',\n" +
                        "  'fields.end_time.ignore-retract' = 'true',\n" +
                        "  'fields.is_current.aggregate-function' = 'last_value',\n" +
                        "  'fields.is_current.ignore-retract' = 'true',\n" +
                        "  'fields.version.aggregate-function' = 'max',\n" +
                        "  'fields.version.ignore-retract' = 'true',\n" +
                        "  'changelog-producer' = 'lookup',\n" +
                        "  'metrics.enabled' = 'true'\n" +
                        ")"
        );

        // === 3. 读取 CDC 流并转为 POJO ===
        Table cdcTable = tEnv.from("cdc_m_product");
        DataStream<Row> changelogStream = tEnv.toChangelogStream(cdcTable);

        DataStream<ProductRecord> productStream = changelogStream
                .filter(row -> {
                    RowKind kind = row.getKind();
                    return kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER;
                })
                .map(row -> {
                    ProductRecord r = new ProductRecord();
                    r.id = (String) row.getField(0);
                    r.name = (String) row.getField(1);
                    r.value = (String) row.getField(2);
                    r.mDimId = (String) row.getField(3);
                    r.mAttributesetinstanceId = (String) row.getField(4);
                    r.tagPrice = (BigDecimal) row.getField(5);
                    r.guidePrice = (BigDecimal) row.getField(6);
                    r.pdtCost = (BigDecimal) row.getField(7);
                    r.marketDate = (LocalDate) row.getField(8);
                    r.createdAt = (LocalDateTime) row.getField(9);
                    r.updatedAt = (LocalDateTime) row.getField(10);
                    return r;
                });

        // === 4. 应用 SCD2 逻辑 ===
        DataStream<Scd2OutputRecord> scd2Stream = productStream
                .keyBy(r -> r.id)
                .process(new Scd2ProductProcessFunctionV2());

        // === 5. 转为 Table 并写入 Paimon ===
        // 构建嵌套 ROW 类型（与目标表 row_data 结构一致）
        DataType rowDataDataType = ROW(
                FIELD("name", STRING()),
                FIELD("value", STRING()),
                FIELD("m_dim_id", STRING()),
                FIELD("m_attributesetinstance_id", STRING()),
                FIELD("tag_price", DECIMAL(12, 2)),
                FIELD("guide_price", DECIMAL(12, 2)),
                FIELD("pdt_cost", DECIMAL(12, 2)),
                FIELD("market_date", DATE()),
                FIELD("created_at", TIMESTAMP(3)),
                FIELD("updated_at", TIMESTAMP(3))
        );

        // 构建完整 Schema
        Schema scd2Schema = Schema.newBuilder()
                .column("id", STRING())
                .column("row_data", rowDataDataType)      // 注意：字段名必须和 POJO 中的 public 字段名一致
                .column("startTime", TIMESTAMP(3))
                .column("endTime", TIMESTAMP(3))
                .column("isCurrent", BOOLEAN())
                .column("version", BIGINT())
                .build();

        // 转换为 Table
        Table resultTable = tEnv.fromDataStream(scd2Stream, scd2Schema);
        resultTable.printSchema();
        // 执行 INSERT 并触发作业
        resultTable.executeInsert("ods_m_product_history_v3").await();
        env.execute("SCD2-Product-Job");
    }
}