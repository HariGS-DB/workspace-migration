-- Query 6: Active workspaces and paying accounts count for UK South FY26 Q4
SELECT
  COUNT(DISTINCT sfdc_account_id) AS paying_accounts,
  COUNT(DISTINCT sfdc_workspace_id) AS paying_workspaces,
  ROUND(SUM(usage_amount), 2) AS total_dbus,
  ROUND(SUM(usage_dollars), 2) AS total_dollars
FROM main.fin_live_gold.paid_usage_metering
WHERE shard_region = 'uksouth'
  AND platform = 'azure'
  AND paying_status = 'PAYING_STATUS_PAYING'
  AND date >= '2025-12-01' AND date <= '2026-02-28'
