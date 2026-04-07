-- Query 3: UC % by Product Line for UK South FY26 Q4
SELECT product_type,
  ROUND(SUM(workload_insights_metric.uc_dollars), 2) AS uc_dollars,
  ROUND(SUM(usage_dollars), 2) AS total_dollars,
  ROUND(SUM(workload_insights_metric.uc_dollars) / NULLIF(SUM(usage_dollars), 0) * 100, 2) AS uc_pct_dollars,
  ROUND(SUM(workload_insights_metric.uc_dbu), 2) AS uc_dbus,
  ROUND(SUM(usage_amount), 2) AS total_dbus,
  ROUND(SUM(workload_insights_metric.uc_dbu) / NULLIF(SUM(usage_amount), 0) * 100, 2) AS uc_pct_dbus,
  ROUND(SUM(workload_insights_metric.uc_eligible_dollars), 2) AS uc_eligible_dollars,
  ROUND(SUM(workload_insights_metric.uc_dollars) / NULLIF(SUM(workload_insights_metric.uc_eligible_dollars), 0) * 100, 2) AS uc_pct_of_eligible
FROM main.fin_live_gold.paid_usage_metering
WHERE shard_region = 'uksouth'
  AND platform = 'azure'
  AND paying_status = 'PAYING_STATUS_PAYING'
  AND date >= '2025-12-01' AND date <= '2026-02-28'
GROUP BY product_type
HAVING SUM(usage_dollars) > 100
ORDER BY total_dollars DESC
