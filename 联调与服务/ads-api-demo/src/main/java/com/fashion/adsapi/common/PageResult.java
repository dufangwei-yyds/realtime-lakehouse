package com.fashion.adsapi.common;

import java.util.List;

public record PageResult<T>(
        long total,
        Integer pageNo,
        Integer pageSize,
        List<T> records
) {
}
