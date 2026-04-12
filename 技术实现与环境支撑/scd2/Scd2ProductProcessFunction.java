package org.dufangwei.flink.scd2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.time.LocalDateTime;

public class Scd2ProductProcessFunction extends KeyedProcessFunction<String, ProductRecord, Scd2OutputRecord> {

    private ValueState<ProductRecord> currentRecordState;
    private ValueState<Long> versionState;

    @Override
    public void open(Configuration parameters) {
        currentRecordState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentRecord", Types.POJO(ProductRecord.class))
        );
        versionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("version", Types.LONG)
        );
    }

    @Override
    public void processElement(ProductRecord newRecord, Context ctx, Collector<Scd2OutputRecord> out) throws Exception {
        ProductRecord currentRecord = currentRecordState.value();
        Long currentVersion = versionState.value() == null ? 0L : versionState.value();

        if (currentRecord == null) {
            // 首次插入
            emitVersion(newRecord, newRecord.updatedAt, true, currentVersion + 1, null, out);
            currentRecordState.update(newRecord);
            versionState.update(currentVersion + 1);
        } else {
            // 比较是否变化（已重写 equals，不含 updatedAt）
            if (!currentRecord.equals(newRecord)) {
                // 关闭旧版本
                emitVersion(currentRecord, currentRecord.updatedAt, false, currentVersion, newRecord.updatedAt, out);
                // 开启新版本
                emitVersion(newRecord, newRecord.updatedAt, true, currentVersion + 1, null, out);
                // 更新状态
                currentRecordState.update(newRecord);
                versionState.update(currentVersion + 1);
            }
            // 若未变化，忽略（避免冗余版本）
        }
    }

    private void emitVersion(ProductRecord record, LocalDateTime startTime,
                             boolean isCurrent, long version,
                             LocalDateTime endTimeOverride,
                             Collector<Scd2OutputRecord> out) {
        LocalDateTime endTime = (endTimeOverride != null) ?
                endTimeOverride :
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000);

        Row rowData = Row.of(
                record.name,
                record.value,
                record.mDimId,
                record.mAttributesetinstanceId,
                record.tagPrice,
                record.guidePrice,
                record.pdtCost,
                record.marketDate,
                record.createdAt,
                record.updatedAt
        );

        out.collect(new Scd2OutputRecord(record.id, rowData, startTime, endTime, isCurrent, version));
    }
}