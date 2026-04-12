package com.fashion.adsapi.hotsku;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record HotSkuReorderDTO(
        LocalDateTime statHour,
        String skuId,
        String skuName,
        String brandName,
        BigDecimal hotScore,
        Integer rankNo,
        Integer reorderSuggestFlag,
        String reorderLevel,
        String triggerReason,
        Integer stockSupportFlag,
        LocalDateTime updateTime
) {
}
