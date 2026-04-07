-- Query 5a: UC Table inventory by type and data_source_format (az-ukwest shard)
SELECT
  t.type,
  t.data_source_format,
  COUNT(*) AS table_count
FROM main.data_centralized_db_snapshot.managedcatalog__tidb__mc_tables_latest_snapshot t
WHERE t._mrt_snapshot_source_shard = 'az-ukwest'
  AND t.is_deleted = 0
GROUP BY t.type, t.data_source_format
ORDER BY table_count DESC
