package com.fashion.adsapi.inventory;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record InventoryDispatchSuggestionDTO(
        LocalDateTime statMinute,
        String storeId,
        String warehouseId,
        String skuId,
        String skuName,
        BigDecimal atpQty,
        BigDecimal demandQty,
        BigDecimal dispatchPriorityScore,
        String suggestAction,
        String triggerReason,
        LocalDateTime updateTime
) {
}
