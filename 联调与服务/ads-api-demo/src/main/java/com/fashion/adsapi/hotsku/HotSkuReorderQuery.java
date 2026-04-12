package com.fashion.adsapi.hotsku;

public record HotSkuReorderQuery(
        String brandName,
        String productType,
        Integer pageNo,
        Integer pageSize
) {
}
