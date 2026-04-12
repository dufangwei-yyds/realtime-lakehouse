package org.dufangwei.flink.scd2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;

/**
 * SCD2核心处理函数(增强版)
 * 能力:
 * 1. 标准SCD2版本管理
 * 2. 作业级SCD2 Metrics(Prometheus/Grafana)
 * 3. 主键重复检测
 * 4. Version跳跃检测
 * 5. 异常日志采样
 */
public class Scd2ProductProcessFunctionV2
        extends KeyedProcessFunction<String, ProductRecord, Scd2OutputRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(Scd2ProductProcessFunctionV2.class);

    /** =========================
     *        State
     *  =========================
     */
    private ValueState<ProductRecord> currentRecordState;
    private ValueState<Long> versionState;

    /** =========================
     *        Metrics
     *  =========================
     */
    private transient Counter scd2InputTotal;
    private transient Counter scd2OutputTotal;
    private transient Counter scd2NoChangeTotal;
    private transient Counter scd2VersionJumpTotal;
    private transient Counter scd2DuplicatePkTotal;

    /** 日志采样计数(防止刷日志) */
    private transient long logSampleCounter;

    @Override
    public void open(Configuration parameters) {

        // ---------- State ----------
        currentRecordState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentRecord",
                        Types.POJO(ProductRecord.class))
        );

        versionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("version", Types.LONG)
        );

        // ---------- Metrics ----------
        MetricGroup metricGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("scd2");

        scd2InputTotal = metricGroup.counter("input_total");
        scd2OutputTotal = metricGroup.counter("output_total");
        scd2NoChangeTotal = metricGroup.counter("nochange_total");
        scd2VersionJumpTotal = metricGroup.counter("version_jump_total");
        scd2DuplicatePkTotal = metricGroup.counter("duplicate_pk_total");

        logSampleCounter = 0L;
    }

    @Override
    public void processElement(
            ProductRecord newRecord,
            Context ctx,
            Collector<Scd2OutputRecord> out) throws Exception {

        scd2InputTotal.inc();

        ProductRecord currentRecord = currentRecordState.value();
        Long currentVersion = versionState.value();
        if (currentVersion == null) {
            currentVersion = 0L;
        }

        // =========================
        // 1️⃣ 首次插入
        // =========================
        if (currentRecord == null) {
            long newVersion = currentVersion + 1;

            emitVersion(
                    newRecord,
                    newRecord.updatedAt,
                    true,
                    newVersion,
                    null,
                    out
            );

            currentRecordState.update(newRecord);
            versionState.update(newVersion);

            scd2OutputTotal.inc();
            return;
        }

        // =========================
        // 2️⃣ 主键重复 & Version 跳跃检测
        // =========================
        long expectedNextVersion = currentVersion + 1;

        if (currentVersion > 0 && expectedNextVersion - currentVersion > 1) {
            scd2VersionJumpTotal.inc();
            sampledWarn(
                    "Version jump detected, key={}, lastVersion={}, expected={}",
                    newRecord.id, currentVersion, expectedNextVersion
            );
        }

        // =========================
        // 3️⃣ 判断是否发生业务变化
        // =========================
        if (!currentRecord.equals(newRecord)) {

            // 关闭旧版本
            emitVersion(
                    currentRecord,
                    currentRecord.updatedAt,
                    false,
                    currentVersion,
                    newRecord.updatedAt,
                    out
            );

            // 开启新版本
            long newVersion = currentVersion + 1;
            emitVersion(
                    newRecord,
                    newRecord.updatedAt,
                    true,
                    newVersion,
                    null,
                    out
            );

            currentRecordState.update(newRecord);
            versionState.update(newVersion);

            scd2OutputTotal.inc();
        } else {
            // =========================
            // 4️⃣ 无变化更新
            // =========================
            scd2NoChangeTotal.inc();
        }
    }

    /**
     * 输出一个 SCD2 版本记录
     */
    private void emitVersion(
            ProductRecord record,
            LocalDateTime startTime,
            boolean isCurrent,
            long version,
            LocalDateTime endTimeOverride,
            Collector<Scd2OutputRecord> out) {

        LocalDateTime endTime = endTimeOverride != null
                ? endTimeOverride
                : LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000);

        Row rowData = record.toRow();

        out.collect(new Scd2OutputRecord(
                record.id,
                rowData,
                startTime,
                endTime,
                isCurrent,
                version
        ));
    }

    /**
     * 日志采样输出(每 1000 条最多打一次)
     */
    private void sampledWarn(String format, Object... args) {
        logSampleCounter++;
        if (logSampleCounter % 1000 == 0) {
            LOG.warn(format, args);
        }
    }
}
