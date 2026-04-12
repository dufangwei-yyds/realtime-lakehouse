package com.fashion.adsapi.hotsku;

import com.fashion.adsapi.common.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class HotSkuReorderRepository {

    private final JdbcTemplate jdbcTemplate;

    public HotSkuReorderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public long count(HotSkuReorderQuery query) {
        String sql = """
            SELECT COUNT(1)
            FROM ads.ads_hot_sku_reorder_api
            WHERE reorder_suggest_flag = 1
              AND (? IS NULL OR brand_name = ?)
              AND (? IS NULL OR product_type = ?)
            """;
        Long total = jdbcTemplate.queryForObject(
                sql,
                Long.class,
                query.brandName(),
                query.brandName(),
                query.productType(),
                query.productType()
        );
        return total == null ? 0L : total;
    }

    public List<HotSkuReorderDTO> query(HotSkuReorderQuery query, PageRequest pageRequest) {
        String sql = """
            SELECT
                stat_hour,
                sku_id,
                sku_name,
                brand_name,
                hot_score,
                rank_no,
                reorder_suggest_flag,
                reorder_level,
                trigger_reason,
                stock_support_flag,
                update_time
            FROM ads.ads_hot_sku_reorder_api
            WHERE reorder_suggest_flag = 1
              AND (? IS NULL OR brand_name = ?)
              AND (? IS NULL OR product_type = ?)
            ORDER BY hot_score DESC, rank_no ASC
            LIMIT ? OFFSET ?
            """;

        return jdbcTemplate.query(
                sql,
                new Object[]{
                        query.brandName(), query.brandName(),
                        query.productType(), query.productType(),
                        pageRequest.safePageSize(), pageRequest.offset()
                },
                (rs, rowNum) -> new HotSkuReorderDTO(
                        rs.getTimestamp("stat_hour").toLocalDateTime(),
                        rs.getString("sku_id"),
                        rs.getString("sku_name"),
                        rs.getString("brand_name"),
                        rs.getBigDecimal("hot_score"),
                        rs.getInt("rank_no"),
                        rs.getInt("reorder_suggest_flag"),
                        rs.getString("reorder_level"),
                        rs.getString("trigger_reason"),
                        rs.getInt("stock_support_flag"),
                        rs.getTimestamp("update_time").toLocalDateTime()
                )
        );
    }
}
