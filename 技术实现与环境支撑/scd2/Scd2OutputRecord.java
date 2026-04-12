package org.dufangwei.flink.scd2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.types.Row;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Scd2OutputRecord {
    public String id;
    public Row row_data;
    public LocalDateTime startTime;
    public LocalDateTime endTime;
    public boolean isCurrent;
    public long version;
}
