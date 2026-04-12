package com.fashion.adsapi.hotsku;

import com.fashion.adsapi.common.PageRequest;
import com.fashion.adsapi.common.PageResult;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HotSkuReorderService {

    private final HotSkuReorderRepository repository;

    public HotSkuReorderService(HotSkuReorderRepository repository) {
        this.repository = repository;
    }

    public PageResult<HotSkuReorderDTO> queryReorderSuggestions(HotSkuReorderQuery query) {
        PageRequest pageRequest = new PageRequest(query.pageNo(), query.pageSize());
        long total = repository.count(query);
        List<HotSkuReorderDTO> records = repository.query(query, pageRequest);
        return new PageResult<>(total, pageRequest.safePageNo(), pageRequest.safePageSize(), records);
    }
}
