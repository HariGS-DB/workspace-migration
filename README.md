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

1. Clone this repo
2. `databricks bundle deploy -t dev` (uploads `config.yaml` with
   placeholders to the workspace)
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

Same shape as above — edit the workspace `config.yaml` first:

1. `databricks bundle deploy -t dev`
2. Edit `${workspace.file_path}/config.yaml`:
   - Real workspace URLs, SPN app ID, secret scope/key
   - `scope.include_hive: true` (the Hive integration test requires it)
   - `iceberg_strategy: ddl_replay` (if the source has UC-managed Iceberg
     tables in the fixture)
   - `migrate_hive_dbfs_root: true` + `hive_dbfs_target_path:
     abfss://<container>@<account>.dfs.core.windows.net/<path>` for the
     Hive DBFS-root branch (the SPN needs READ_FILES + WRITE_FILES +
     CREATE_EXTERNAL_TABLE on the corresponding external location)
3. Trigger `uc_integration_test` — waits for the SPN+secrets to be set
   up on the source workspace, then runs seed → pre_check → discovery →
   migrate → test → teardown
4. Trigger `hive_integration_test` — same shape for the Hive branch

See [docs/external_hive_metastore.md](docs/external_hive_metastore.md) for
the Hive-specific cluster/init-script reconfiguration checklist.

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
