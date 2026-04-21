# Workspace Migration

A Databricks Asset Bundle (DAB) that migrates Unity Catalog and legacy Hive
Metastore objects between Databricks workspaces — useful for control-plane
migrations, account consolidations, or moving between Azure regions.

## Coverage

**Unity Catalog**
- Catalogs, schemas, grants
- Managed tables (Delta, Iceberg), external tables
- Views, SQL and Python functions
- Volumes (managed with file-level copy, external via metadata replay)
- Materialized views and streaming tables (SQL-created; DLT-owned
  variants are out of scope — migrate those via pipeline migration)
- Tags, row filters, column masks, ABAC policies
- Lakehouse monitors, registered models (metadata + aliases)
- Connections, foreign catalogs, online tables
- Delta Sharing (shares, recipients, providers; objects include tables,
  views, volumes, schemas, catalogs)

**Legacy Hive Metastore**
- Databases, managed + external tables, views, functions, grants
- Separate workflow chain; toggleable via `scope.include_hive`

## Layout

```
.
├── databricks.yml              # Bundle root
├── config.yaml                 # Customer-editable runtime config
├── config.example.yaml         # Reference
├── resources/                  # Workflow + dashboard definitions
├── src/
│   ├── common/                 # auth, catalog_utils, tracking, sql_utils, validation
│   ├── pre_check/              # pre-migration validation
│   ├── discovery/              # inventory source workspace
│   └── migrate/                # per-object-type workers + orchestrator
├── tests/{unit,connect,integration,lint}
├── scripts/                    # CI helpers
└── dashboards/
```

## Usage

`config.yaml` ships in the repo with **placeholder values only** so the
source tree never carries environment-specific identifiers. The
authoritative config is the copy on the workspace at
`${workspace.file_path}/config.yaml`, which DAB refreshes from the repo
on every deploy.

### Required deploy-time variables

`databricks.yml` declares two variables with no baked-in defaults —
operators must supply them for every deploy:

| Variable | Purpose | How to set |
|---|---|---|
| `migration_spn_id` | SPN application ID that jobs run as | `--var migration_spn_id=<app-id>` or env `BUNDLE_VAR_migration_spn_id` |
| `dashboard_warehouse_name` | Name of the SQL warehouse the dashboard reads from (resolved to an ID via lookup). Defaults to `cp-migration` — override if your warehouse has a different name | `--var dashboard_warehouse_name=<name>` or env `BUNDLE_VAR_dashboard_warehouse_name` |

The SPN needs: workspace admin on source + target, metastore-level
`CREATE_*` privileges, `USE_PROVIDER` on target, and
`READ_FILES`/`WRITE_FILES`/`CREATE_EXTERNAL_TABLE` on any external
location used for Hive DBFS-root migration.

### Deploy + configure flow

1. Clone this repo
2. `databricks bundle deploy -t dev --var migration_spn_id=<your-app-id>`
   (uploads `config.yaml` with placeholders to the workspace)
3. In the workspace, edit `${workspace.file_path}/config.yaml` with real
   values:
   - `source_workspace_url` / `target_workspace_url`
   - `spn_client_id` + `spn_secret_scope`/`spn_secret_key` (OAuth service
     principal with access to both workspaces)
   - `scope.include_uc` / `scope.include_hive`
   - optional: `catalog_filter`, `schema_filter`, `iceberg_strategy`,
     `migrate_hive_dbfs_root`, `hive_dbfs_target_path`
4. Run the `pre_check` workflow to validate connectivity and grants
5. Run `discovery` to inventory source objects
6. Run `migrate` to replay on target

> **Note:** a subsequent `databricks bundle deploy` will overwrite the
> workspace `config.yaml` with the placeholder version again. Re-apply
> your edits after each deploy, or maintain your real values in an
> out-of-repo copy that you paste in when needed.

### Running the integration tests

The integration workflows override the workspace `config.yaml` per-run
(backup before, restore in teardown), so you only need to populate the
environment-specific fields **once** after deploy.

1. `databricks bundle deploy -t dev --var migration_spn_id=<your-app-id>`
2. Edit `${workspace.file_path}/config.yaml` with the environment-specific
   fields once:
   - Real workspace URLs, SPN app ID, secret scope/key
   - `hive_dbfs_target_path: abfss://<container>@<account>.dfs.core.windows.net/<path>`
     (the SPN needs `READ_FILES` + `WRITE_FILES` +
     `CREATE_EXTERNAL_TABLE` on the corresponding external location)
3. Trigger `uc_integration_test` — the first task (`setup_test_config`)
   rewrites the workspace config.yaml with UC-appropriate toggles
   (`include_uc=true`, `include_hive=false`, `iceberg_strategy=ddl_replay`),
   runs seed → pre_check → discovery → migrate → test, and `teardown_uc`
   restores the original config.yaml from the backup.
4. Trigger `hive_integration_test` — same pattern, with Hive-appropriate
   toggles (`include_hive=true`, `migrate_hive_dbfs_root=true`,
   `iceberg_strategy=""`). Your operator-set `hive_dbfs_target_path`
   from step 2 is preserved — workflows don't overwrite env-specific
   paths, only the behavioral toggles.

The per-workflow toggles live in each workflow's YAML task parameters;
edit them there if you need to change test behavior.

See [docs/external_hive_metastore.md](docs/external_hive_metastore.md) for
the Hive-specific cluster/init-script reconfiguration checklist.

## Row filter / column mask on managed tables

Delta Sharing providers cannot share tables protected by legacy
row-level security or column masks — i.e. anything applied via
`ALTER TABLE ... SET ROW FILTER` or `ALTER COLUMN ... SET MASK`. The
Delta Sharing API rejects such tables with:

```
InvalidParameterValue: Table <fqn> has row level security or column masks,
which is not supported by Delta Sharing.
```

Because this tool uses Delta Sharing to move managed-table data between
workspaces, affected tables can't flow through the standard path.

### Default behavior (safe skip)

With `rls_cm_strategy: ""` (the default), discovery surfaces a warning
listing the affected tables, and `setup_sharing` excludes them from the
share. `migration_status` records one row per skipped table with
`status = skipped_by_rls_cm_policy` so the skip is auditable from the
dashboard and the test suite. **The skipped tables' data does not move
to target.** Schema and grants on those tables still migrate, but the
table itself arrives on target empty (or doesn't arrive at all,
depending on whether a prior migration created it).

### Your options

1. **Migrate governance to ABAC first.** Delta Sharing *does* support
   sharing tables protected by Unity Catalog ABAC row filter and column
   mask policies (the caller must be exempt from the policy). Rewrite
   the affected tables' RLS/CM as ABAC policies on source before
   running this tool. Recipients can also apply their own ABAC-based
   RLS/CM on the shared tables on target.

2. **Accept the skip** and re-populate the affected tables by other
   means after the migration (e.g. point queries at source during
   cutover, or rebuild from upstream).

3. **Opt into `rls_cm_strategy: drop_and_restore`** — *NOT YET
   IMPLEMENTED*. The planned flow is:
   - On source: save the current RLS/CM definition, then
     `ALTER TABLE ... DROP ROW FILTER` / `DROP MASK`.
   - Add the table to the migration share and DEEP CLONE to target.
   - On source: reapply the saved RLS/CM definition.
   - On target: the existing `row_filters_worker` / `column_masks_worker`
     apply the filter/mask from discovery_inventory.

   **Risk**: between the source drop and the source restore, the table
   is unprotected. Any concurrent reader on source can see unfiltered,
   unmasked data. Window is typically seconds to minutes per table
   depending on DEEP CLONE duration. This path will only be appropriate
   for maintenance-window migrations, not live ones. The implementation
   will include a tracker-backed recovery harness so a crashed migration
   auto-restores source state on restart.

   The config flag is accepted today but raises `NotImplementedError`
   at `setup_sharing` time so nobody silently flips it on before the
   implementation lands.

## Architecture

- All workflows run on serverless compute
- Delta Sharing is used to move managed-table bytes between workspaces
  (`DEEP CLONE` from a share-consumer catalog on target)
- Three Delta tracking tables in `migration_tracking.cp_migration`:
  `discovery_inventory`, `migration_status`, `pre_check_results`
- A Lakeview dashboard surfaces counts, failures, and durations per
  object type

## License

See [LICENSE.md](LICENSE.md).
