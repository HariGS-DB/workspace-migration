-- Query 2: UC % by Dollars, DBUs, and Workspace Count for UK South FY26 Q4
WITH ws_uc AS (
  SELECT
    sfdc_workspace_id,
    SUM(workload_insights_metric.uc_dollars) AS uc_dollars,
    SUM(usage_dollars) AS total_dollars,
    SUM(workload_insights_metric.uc_dbu) AS uc_dbus,
    SUM(usage_amount) AS total_dbus,
    SUM(workload_insights_metric.uc_eligible_dollars) AS uc_eligible_dollars,
    SUM(workload_insights_metric.uc_eligible_dbu) AS uc_eligible_dbus
  FROM main.fin_live_gold.paid_usage_metering
  WHERE shard_region = 'uksouth'
    AND platform = 'azure'
    AND paying_status = 'PAYING_STATUS_PAYING'
    AND date >= '2025-12-01' AND date <= '2026-02-28'
  GROUP BY sfdc_workspace_id
)
SELECT 'By Dollars' AS metric,
  ROUND(SUM(uc_dollars), 2) AS uc_value,
  ROUND(SUM(total_dollars), 2) AS total_value,
  ROUND(SUM(uc_dollars) / NULLIF(SUM(total_dollars), 0) * 100, 2) AS uc_pct,
  ROUND(SUM(uc_eligible_dollars), 2) AS eligible_value,
  ROUND(SUM(uc_dollars) / NULLIF(SUM(uc_eligible_dollars), 0) * 100, 2) AS uc_pct_of_eligible
FROM ws_uc
UNION ALL
SELECT 'By DBUs',
  ROUND(SUM(uc_dbus), 2), ROUND(SUM(total_dbus), 2),
  ROUND(SUM(uc_dbus) / NULLIF(SUM(total_dbus), 0) * 100, 2),
  ROUND(SUM(uc_eligible_dbus), 2),
  ROUND(SUM(uc_dbus) / NULLIF(SUM(uc_eligible_dbus), 0) * 100, 2)
FROM ws_uc
UNION ALL
SELECT 'By Workspace Count',
  CAST(SUM(CASE WHEN uc_dollars > 0 THEN 1 ELSE 0 END) AS DOUBLE),
  CAST(COUNT(*) AS DOUBLE),
  ROUND(SUM(CASE WHEN uc_dollars > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
  CAST(SUM(CASE WHEN uc_eligible_dollars > 0 THEN 1 ELSE 0 END) AS DOUBLE),
  ROUND(SUM(CASE WHEN uc_dollars > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN uc_eligible_dollars > 0 THEN 1 ELSE 0 END), 0), 2)
FROM ws_uc
