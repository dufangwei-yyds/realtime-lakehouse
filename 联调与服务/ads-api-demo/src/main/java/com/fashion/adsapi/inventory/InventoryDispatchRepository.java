package com.fashion.adsapi.inventory;

import com.fashion.adsapi.common.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class InventoryDispatchRepository {

    private final JdbcTemplate jdbcTemplate;

    public InventoryDispatchRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public long count(InventoryDispatchQuery query) {
        String sql = """
            SELECT COUNT(1)
            FROM ads.ads_inventory_dispatch_api
            WHERE shortage_risk_flag = 1
              AND (? IS NULL OR store_id = ?)
              AND (? IS NULL OR warehouse_id = ?)
            """;
        Long total = jdbcTemplate.queryForObject(
                sql,
                Long.class,
                query.storeId(),
                query.storeId(),
                query.warehouseId(),
                query.warehouseId()
        );
        return total == null ? 0L : total;
    }

    public List<InventoryDispatchSuggestionDTO> query(InventoryDispatchQuery query, PageRequest pageRequest) {
        String sql = """
            SELECT
                stat_minute,
                store_id,
                warehouse_id,
                sku_id,
                sku_name,
                atp_qty,
                demand_qty,
                dispatch_priority_score,
                suggest_action,
                trigger_reason,
                update_time
            FROM ads.ads_inventory_dispatch_api
            WHERE shortage_risk_flag = 1
              AND (? IS NULL OR store_id = ?)
              AND (? IS NULL OR warehouse_id = ?)
            ORDER BY dispatch_priority_score DESC, stat_minute DESC
            LIMIT ? OFFSET ?
            """;

        return jdbcTemplate.query(
                sql,
                new Object[]{
                        query.storeId(), query.storeId(),
                        query.warehouseId(), query.warehouseId(),
                        pageRequest.safePageSize(), pageRequest.offset()
                },
                (rs, rowNum) -> new InventoryDispatchSuggestionDTO(
                        rs.getTimestamp("stat_minute").toLocalDateTime(),
                        rs.getString("store_id"),
                        rs.getString("warehouse_id"),
                        rs.getString("sku_id"),
                        rs.getString("sku_name"),
                        rs.getBigDecimal("atp_qty"),
                        rs.getBigDecimal("demand_qty"),
                        rs.getBigDecimal("dispatch_priority_score"),
                        rs.getString("suggest_action"),
                        rs.getString("trigger_reason"),
                        rs.getTimestamp("update_time").toLocalDateTime()
                )
        );
    }
}
