package com.fashion.adsapi.hotsku;

import com.fashion.adsapi.common.ApiResponse;
import com.fashion.adsapi.common.PageResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ads/hot-sku")
public class HotSkuReorderController {

    private final HotSkuReorderService service;

    public HotSkuReorderController(HotSkuReorderService service) {
        this.service = service;
    }

    @GetMapping("/reorder-suggestions")
    public ApiResponse<PageResult<HotSkuReorderDTO>> queryReorderSuggestions(
            @RequestParam(required = false) String brandName,
            @RequestParam(required = false) String productType,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "20") Integer pageSize) {

        HotSkuReorderQuery query = new HotSkuReorderQuery(brandName, productType, pageNo, pageSize);
        return ApiResponse.success(service.queryReorderSuggestions(query));
    }
}
