#!/usr/bin/env bash
# Write /Workspace/Shared/cp_migration/config.yaml from env vars.
# Used by CI / integration tests to seed the config file before running the
# bundle workflows. Humans use the bootstrap workflow + Databricks UI instead.
#
# Required env vars:
#   SOURCE_WORKSPACE_URL, TARGET_WORKSPACE_URL, SPN_CLIENT_ID
# Optional env vars (defaults shown):
#   SPN_SECRET_SCOPE=migration, SPN_SECRET_KEY=spn-secret
#   CATALOG_FILTER=integration_test_src, SCHEMA_FILTER=
#   TRACKING_CATALOG=migration_tracking, TRACKING_SCHEMA=cp_migration
#   DRY_RUN=false, BATCH_SIZE=50
#   MIGRATE_HIVE_DBFS_ROOT=false, HIVE_DBFS_TARGET_PATH=, HIVE_TARGET_CATALOG=hive_upgraded
#   PROFILE=source-migration (Databricks CLI profile used to upload the file)
set -euo pipefail

: "${SOURCE_WORKSPACE_URL:?required}"
: "${TARGET_WORKSPACE_URL:?required}"
: "${SPN_CLIENT_ID:?required}"

: "${SPN_SECRET_SCOPE:=migration}"
: "${SPN_SECRET_KEY:=spn-secret}"
: "${CATALOG_FILTER:=integration_test_src}"
: "${SCHEMA_FILTER:=}"
: "${TRACKING_CATALOG:=migration_tracking}"
: "${TRACKING_SCHEMA:=cp_migration}"
: "${DRY_RUN:=false}"
: "${BATCH_SIZE:=50}"
: "${MIGRATE_HIVE_DBFS_ROOT:=false}"
: "${HIVE_DBFS_TARGET_PATH:=}"
: "${HIVE_TARGET_CATALOG:=hive_upgraded}"
: "${PROFILE:=source-migration}"

TMPFILE=$(mktemp -t cp_migration_config).yaml
trap 'rm -f "$TMPFILE"' EXIT

cat > "$TMPFILE" <<EOF
source_workspace_url: ${SOURCE_WORKSPACE_URL}
target_workspace_url: ${TARGET_WORKSPACE_URL}
spn_client_id: ${SPN_CLIENT_ID}
spn_secret_scope: ${SPN_SECRET_SCOPE}
spn_secret_key: ${SPN_SECRET_KEY}
catalog_filter: "${CATALOG_FILTER}"
schema_filter: "${SCHEMA_FILTER}"
tracking_catalog: ${TRACKING_CATALOG}
tracking_schema: ${TRACKING_SCHEMA}
dry_run: ${DRY_RUN}
batch_size: ${BATCH_SIZE}
migrate_hive_dbfs_root: ${MIGRATE_HIVE_DBFS_ROOT}
hive_dbfs_target_path: "${HIVE_DBFS_TARGET_PATH}"
hive_target_catalog: ${HIVE_TARGET_CATALOG}
EOF

databricks workspace mkdirs /Shared/cp_migration -p "$PROFILE" >/dev/null || true
databricks workspace import --format=AUTO --overwrite --file="$TMPFILE" /Shared/cp_migration/config.yaml -p "$PROFILE"
echo "✅ Wrote /Workspace/Shared/cp_migration/config.yaml (profile=$PROFILE)"
