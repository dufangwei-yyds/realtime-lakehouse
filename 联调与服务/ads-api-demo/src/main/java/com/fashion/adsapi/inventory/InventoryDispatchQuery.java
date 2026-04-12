package com.fashion.adsapi.inventory;

public record InventoryDispatchQuery(
        String storeId,
        String warehouseId,
        Integer pageNo,
        Integer pageSize
) {
}
