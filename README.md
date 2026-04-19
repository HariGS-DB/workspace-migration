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

1. Clone this repo
2. Edit `config.yaml`:
   - `source_workspace_url` / `target_workspace_url`
   - `spn_client_id` + `spn_secret_scope`/`spn_secret_key` (OAuth service
     principal with access to both workspaces)
   - `scope.include_uc` / `scope.include_hive`
   - optional: `catalog_filter`, `schema_filter`, `iceberg_strategy`
3. `databricks bundle deploy -t dev`
4. Run the `pre_check` workflow to validate connectivity and grants
5. Run `discovery` to inventory source objects
6. Run `migrate` to replay on target

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
