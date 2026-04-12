package org.dufangwei.flink.scd2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.types.Row;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ProductRecord {
    public String id;
    public String name;
    public String value;
    public String mDimId;
    public String mAttributesetinstanceId;
    public BigDecimal tagPrice;
    public BigDecimal guidePrice;
    public BigDecimal pdtCost;
    public LocalDate marketDate;
    public LocalDateTime createdAt;
    public LocalDateTime updatedAt;

    public Row toRow() {
        return Row.of(
                name, value, mDimId, mAttributesetinstanceId,
                tagPrice, guidePrice, pdtCost,
                marketDate, createdAt, updatedAt
        );
    }
}
