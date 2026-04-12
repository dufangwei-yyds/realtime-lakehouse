package com.fashion.adsapi.inventory;

import com.fashion.adsapi.common.PageRequest;
import com.fashion.adsapi.common.PageResult;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InventoryDispatchService {

    private final InventoryDispatchRepository repository;

    public InventoryDispatchService(InventoryDispatchRepository repository) {
        this.repository = repository;
    }

    public PageResult<InventoryDispatchSuggestionDTO> queryDispatchSuggestions(InventoryDispatchQuery query) {
        PageRequest pageRequest = new PageRequest(query.pageNo(), query.pageSize());
        long total = repository.count(query);
        List<InventoryDispatchSuggestionDTO> records = repository.query(query, pageRequest);
        return new PageResult<>(total, pageRequest.safePageNo(), pageRequest.safePageSize(), records);
    }
}
