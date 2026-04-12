package com.fashion.adsapi.inventory;

import com.fashion.adsapi.common.ApiResponse;
import com.fashion.adsapi.common.PageResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ads/inventory")
public class InventoryDispatchController {

    private final InventoryDispatchService service;

    public InventoryDispatchController(InventoryDispatchService service) {
        this.service = service;
    }

    @GetMapping("/dispatch-suggestions")
    public ApiResponse<PageResult<InventoryDispatchSuggestionDTO>> queryDispatchSuggestions(
            @RequestParam(required = false) String storeId,
            @RequestParam(required = false) String warehouseId,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "20") Integer pageSize) {

        InventoryDispatchQuery query = new InventoryDispatchQuery(storeId, warehouseId, pageNo, pageSize);
        return ApiResponse.success(service.queryDispatchSuggestions(query));
    }
}
