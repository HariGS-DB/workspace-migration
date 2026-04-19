#!/usr/bin/env bash
# Write tool/config.yaml from env vars.
# Used by CI / integration tests to seed the bundle config before running
# ``databricks bundle deploy`` — DAB then syncs config.yaml onto the
# workspace alongside the rest of the bundle files. Humans edit
# tool/config.yaml directly in their fork and redeploy.
#
# Required env vars:
#   SOURCE_WORKSPACE_URL, TARGET_WORKSPACE_URL, SPN_CLIENT_ID
# Optional env vars (defaults shown):
#   SPN_SECRET_SCOPE=migration, SPN_SECRET_KEY=spn-secret
#   CATALOG_FILTER=integration_test_src, SCHEMA_FILTER=
#   TRACKING_CATALOG=migration_tracking, TRACKING_SCHEMA=cp_migration
#   DRY_RUN=false, BATCH_SIZE=50
#   MIGRATE_HIVE_DBFS_ROOT=false, HIVE_DBFS_TARGET_PATH=, HIVE_TARGET_CATALOG=hive_upgraded
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

# Script lives at tool/scripts/write_config.sh — write to tool/config.yaml.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${SCRIPT_DIR}/../config.yaml"

cat > "${CONFIG_PATH}" <<EOF
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

echo "Wrote ${CONFIG_PATH} — run 'databricks bundle deploy' to sync to the workspace."
