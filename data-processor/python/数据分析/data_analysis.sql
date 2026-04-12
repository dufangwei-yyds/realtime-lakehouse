-- 1. 热门款式趋势分析 (指导库存): 近7天热度增长最快的款式
SELECT
  style_name,
  sum(hot_score) AS total_hot,
  (sum(hot_score) - lag(sum(hot_score), 7) OVER (ORDER BY crawl_date)) / lag(sum(hot_score), 7) OVER (ORDER BY crawl_date) AS growth_rate
FROM processed.hot_style_top10
WHERE crawl_date >= current_date - 7
GROUP BY style_name
ORDER BY growth_rate DESC
LIMIT 5;
-- 业务决策: 热度增长率 > 100% 的款式 (如 "法式碎花裙"), 门店备货量增加 50%

-- 2. 天气对客流的影响分析: 分析降水概率与客流的关系
SELECT
  case when rain_prob >= 50 then '雨天' else '非雨天' end as weather_type,
  avg(flow_count) as avg_flow,
  count(date) as days
FROM processed.weather_customer
GROUP BY weather_type;
-- 业务决策: 雨天客流下降 20%, 提前推送 "雨天到店赠伞" 的促销活动, 提升到店率




