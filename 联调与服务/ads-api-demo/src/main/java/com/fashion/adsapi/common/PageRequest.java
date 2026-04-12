package com.fashion.adsapi.common;

public record PageRequest(
        Integer pageNo,
        Integer pageSize
) {
    public int safePageNo() {
        return pageNo == null || pageNo < 1 ? 1 : pageNo;
    }

    public int safePageSize() {
        return pageSize == null || pageSize < 1 ? 20 : Math.min(pageSize, 200);
    }

    public int offset() {
        return (safePageNo() - 1) * safePageSize();
    }
}
