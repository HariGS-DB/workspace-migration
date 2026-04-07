-- Query 4: Top 20 accounts by non-UC spend for UK South FY26 Q4
SELECT
  sfdc_account_id,
  sfdc_account_name,
  ROUND(SUM(usage_dollars), 2) AS total_dollars,
  ROUND(SUM(workload_insights_metric.uc_dollars), 2) AS uc_dollars,
  ROUND(SUM(usage_dollars) - SUM(workload_insights_metric.uc_dollars), 2) AS non_uc_dollars,
  ROUND(SUM(workload_insights_metric.uc_dollars) / NULLIF(SUM(usage_dollars), 0) * 100, 2) AS uc_pct,
  ROUND(SUM(workload_insights_metric.uc_eligible_dollars), 2) AS uc_eligible_dollars,
  ROUND(SUM(workload_insights_metric.uc_dollars) / NULLIF(SUM(workload_insights_metric.uc_eligible_dollars), 0) * 100, 2) AS uc_pct_of_eligible
FROM main.fin_live_gold.paid_usage_metering
WHERE shard_region = 'uksouth'
  AND platform = 'azure'
  AND paying_status = 'PAYING_STATUS_PAYING'
  AND date >= '2025-12-01' AND date <= '2026-02-28'
GROUP BY sfdc_account_id, sfdc_account_name
ORDER BY non_uc_dollars DESC
LIMIT 20
