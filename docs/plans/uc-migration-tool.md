# UC Data Migration Tool — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DAB-packaged tool that migrates the full Unity Catalog surface — catalogs, schemas, managed/external tables, volumes, views, functions, grants, tags, ABAC policies, row filters, column masks, registered models, Lakehouse monitors, connections, foreign catalogs, Delta Sharing objects, online tables, comments and table properties — between Azure Databricks workspaces during control plane migrations. Covers pre-check validation, discovery, parallel batched migration, inline validation, and a Lakeview dashboard.

**Phases:**
- **Phase 1 (Tasks 1–14):** Core UC object types — catalogs, schemas, tables, volumes, views, functions, grants.
- **Phase 2 (Tasks 15–26):** Dashboard polish + Hive data migration (legacy `hive_metastore` catalog → UC on target).
- **Phase 3 (Tasks 27–41):** Extended UC governance — tags, ABAC policies, row filters, column masks, monitors, registered models, connections, foreign catalogs, Delta Sharing objects, online tables, comments/properties.

**Architecture:** Three Databricks Workflows (pre-check, discovery, migrate) backed by three Delta tracking tables. The migrate workflow uses `for_each_task` for parallel batched execution, with separate task chains per object type organised into dependency tiers. Delta Sharing enables managed table migration via DEEP CLONE. SPN with OAuth secret authenticates to both source and target workspaces. All UC object types — including governance (tags, ABAC, row filters, column masks) and metadata (comments, properties) — are handled by this tool rather than split with the Terraform exporter (see [terraform_exporter_analysis.md](../terraform_exporter_analysis.md)).

**Tech Stack:** Python, Databricks SDK, Databricks Asset Bundles (DAB), Delta Lake, Delta Sharing, Lakeview Dashboards, Databricks Workflows with `for_each_task`.

---

## File Structure

```
uksouth_migration/tool/
├── databricks.yml                          # DAB root config
├── resources/
│   ├── pre_check_workflow.yml              # Pre-check job definition
│   ├── discovery_workflow.yml              # Discovery job definition
│   └── migrate_workflow.yml                # Migrate + validate job definition
├── src/
│   ├── common/
│   │   ├── config.py                       # Shared config — reads job parameters, builds auth clients
│   │   ├── auth.py                         # SPN OAuth authentication to source + target workspaces
│   │   ├── tracking.py                     # Migration tracking table CRUD
│   │   └── catalog_utils.py               # Catalog/schema/table introspection helpers
│   ├── pre_check/
│   │   └── pre_check.py                    # Pre-check notebook
│   ├── discovery/
│   │   └── discovery.py                    # Discovery notebook
│   └── migrate/
│       ├── setup_sharing.py                # Create shares, recipients, target catalogs/schemas (Phase 1)
│       ├── orchestrator.py                 # Build batches per object type, output task values (Phase 1)
│       ├── managed_table_worker.py         # DEEP CLONE managed tables via delta sharing (Phase 1)
│       ├── external_table_worker.py        # Recreate external table metadata on target (Phase 1)
│       ├── volume_worker.py                # Migrate volumes (external: metadata, managed: data copy) (Phase 1)
│       ├── functions_worker.py             # Replay function DDLs on target (Phase 1)
│       ├── views_worker.py                 # Replay view DDLs in dependency order on target (Phase 1)
│       ├── grants_worker.py                # Replay GRANT statements on target (Phase 1)
│       ├── hive_external_worker.py         # Hive external → UC external (DDL replay, same storage) (Phase 2)
│       ├── hive_managed_nondbfs_worker.py  # Hive managed non-DBFS → UC external (re-register) (Phase 2)
│       ├── hive_managed_dbfs_worker.py     # Hive managed DBFS-root → UC managed (data copy) (Phase 2)
│       ├── hive_views_worker.py            # Replay Hive view DDLs, rewrite namespaces to UC (Phase 2)
│       ├── hive_functions_worker.py        # Replay Hive function DDLs on UC (Phase 2)
│       ├── hive_grants_worker.py           # Replay legacy Hive ACLs as UC grants (Phase 2)
│       ├── tags_worker.py                  # ALTER ... SET TAGS for catalog/schema/table/column/volume (Phase 3)
│       ├── row_filters_worker.py           # ALTER TABLE ... SET ROW FILTER (Phase 3)
│       ├── column_masks_worker.py          # ALTER ... ALTER COLUMN ... SET MASK (Phase 3)
│       ├── policies_worker.py              # REST POST for ABAC policies (Phase 3)
│       ├── comments_properties_worker.py   # COMMENT ON / ALTER TABLE SET TBLPROPERTIES for non-Delta (Phase 3)
│       ├── monitors_worker.py              # REST POST for Lakehouse Monitors (Phase 3)
│       ├── models_worker.py                # Registered Models + artifact copy (Phase 3b)
│       ├── connections_worker.py           # Connections + foreign catalogs (Phase 3)
│       ├── online_tables_worker.py         # REST POST for Online Tables; triggers re-sync (Phase 3)
│       ├── sharing_worker.py               # Customer's own shares/recipients/providers (Phase 3)
│       └── summary.py                      # Aggregate results, final validation summary
├── dashboards/
│   └── migration_dashboard.lvdash.json     # Lakeview dashboard definition
└── tests/
    ├── test_config.py
    ├── test_auth.py
    ├── test_tracking.py
    ├── test_catalog_utils.py
    └── test_orchestrator.py
```

---

## Task 1: DAB Scaffold and Configuration

**Files:**
- Create: `uksouth_migration/tool/databricks.yml`
- Create: `uksouth_migration/tool/src/common/config.py`

- [ ] Create directory structure and __init__.py files
- [ ] Create DAB bundle config with all variables
- [ ] Create shared MigrationConfig class that reads widgets/job parameters
- [ ] Commit

---

## Task 2: SPN Authentication Module

**Files:**
- Create: `uksouth_migration/tool/src/common/auth.py`

- [ ] Implement get_source_client() and get_target_client() using Databricks SDK with SPN OAuth
- [ ] Implement test_connectivity() helper
- [ ] Commit

---

## Task 3: Tracking Tables Module

**Files:**
- Create: `uksouth_migration/tool/src/common/tracking.py`

- [ ] Implement init_tracking_tables() — creates catalog, schema, and 3 Delta tables (discovery_inventory, migration_status, pre_check_results)
- [ ] Implement write_discovery_inventory() — overwrite mode
- [ ] Implement append_migration_status() — append mode
- [ ] Implement append_pre_check_results() — append mode
- [ ] Implement get_latest_migration_status() — ROW_NUMBER windowed query for latest per object
- [ ] Implement get_pending_objects() — join discovery with latest status, return unmigrated objects by type
- [ ] Commit

---

## Task 4: Catalog Utilities Module

**Files:**
- Create: `uksouth_migration/tool/src/common/catalog_utils.py`

- [ ] Implement list_catalogs(), list_schemas() with filters
- [ ] Implement classify_tables() — reads information_schema, classifies as managed_table/external_table/view/materialized_view/streaming_table
- [ ] Implement detect_dlt_managed() — checks TBLPROPERTIES for pipelines.pipelineId
- [ ] Implement get_table_row_count(), get_table_size_bytes()
- [ ] Implement get_create_statement() — SHOW CREATE TABLE for views/tables
- [ ] Implement get_function_ddl() — DESCRIBE FUNCTION EXTENDED
- [ ] Implement list_functions(), list_volumes()
- [ ] Implement resolve_view_dependency_order() — topological sort using view_table_usage
- [ ] Implement list_grants() — SHOW GRANTS on catalog/schema/table/function/volume
- [ ] Commit

---

## Task 5: Pre-Check Workflow

**Files:**
- Create: `uksouth_migration/tool/src/pre_check/pre_check.py`
- Create: `uksouth_migration/tool/resources/pre_check_workflow.yml`

- [ ] Implement pre-check notebook with 10 checks:
  1. SPN auth to source
  2. SPN auth to target
  3. Source metastore accessible
  4. Target metastore attached
  5. Delta sharing enabled on source
  6. Delta sharing enabled on target
  7. Catalog filter matches existing catalogs
  8. Storage credentials exist on target (by name match)
  9. External locations exist on target (by name match)
  10. Tracking schema accessible
- [ ] Write results to pre_check_results table with action_required for failures
- [ ] Print pass/fail/warning summary, raise exception if any fail
- [ ] Create workflow YAML definition
- [ ] Commit

---

## Task 6: Discovery Workflow

**Files:**
- Create: `uksouth_migration/tool/src/discovery/discovery.py`
- Create: `uksouth_migration/tool/resources/discovery_workflow.yml`

- [ ] Implement discovery notebook:
  - Iterate catalogs → schemas → classify tables/views
  - Detect DLT-managed objects (pipeline_id)
  - Collect row counts and sizes for tables
  - List functions and volumes
  - Overwrite discovery_inventory table
  - Print summary by object type, flag DLT-managed count
- [ ] Create workflow YAML definition
- [ ] Commit

---

## Task 7: Migrate Workflow — Setup Sharing

**Files:**
- Create: `uksouth_migration/tool/src/migrate/setup_sharing.py`

- [ ] Create/get delta share (cp_migration_share) on source
- [ ] Create/get recipient for target workspace using global_metastore_id
- [ ] Add pending managed tables to share in batches of 100
- [ ] Grant SELECT on share to recipient
- [ ] Create target catalogs if missing (matching source name/storage_root)
- [ ] Create target schemas if missing (matching source name/storage_root)
- [ ] Support dry_run mode
- [ ] Commit

---

## Task 8: Migrate Workflow — Orchestrator

**Files:**
- Create: `uksouth_migration/tool/src/migrate/orchestrator.py`

- [ ] Read discovery_inventory filtered by migration_scope
- [ ] Exclude already migrated/validated objects (idempotency via get_pending_objects)
- [ ] Build separate batch lists per object type: managed_table_batches, external_table_batches, volume_batches
- [ ] Build function_list and view_list (no batching)
- [ ] Output all via dbutils.jobs.taskValues.set()
- [ ] Commit

---

## Task 9: Migrate Workflow — Managed Table Worker

**Files:**
- Create: `uksouth_migration/tool/src/migrate/managed_table_worker.py`

- [ ] Parse batch from for_each_task input (batch_id, objects JSON)
- [ ] Get job_run_id and task_run_id from notebook context
- [ ] Find SQL warehouse on target
- [ ] For each table (ThreadPoolExecutor, 4 threads):
  - Write in_progress to tracking
  - Execute DEEP CLONE on target via statement execution API (async poll)
  - Validate row count match
  - Record status with timing
- [ ] Append results to migration_status
- [ ] Commit

---

## Task 10: Migrate Workflow — External Table Worker

**Files:**
- Create: `uksouth_migration/tool/src/migrate/external_table_worker.py`

- [ ] Parse batch from for_each_task input
- [ ] For each table (ThreadPoolExecutor, 4 threads):
  - Get CREATE statement from source via SHOW CREATE TABLE
  - Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
  - Execute on target
  - Validate row count match
  - Record status with timing
- [ ] Append results to migration_status
- [ ] Commit

---

## Task 11: Migrate Workflow — Volume, Functions, Views, Grants Workers

**Files:**
- Create: `uksouth_migration/tool/src/migrate/volume_worker.py`
- Create: `uksouth_migration/tool/src/migrate/functions_worker.py`
- Create: `uksouth_migration/tool/src/migrate/views_worker.py`
- Create: `uksouth_migration/tool/src/migrate/grants_worker.py`

- [ ] Volume worker: external volumes = metadata recreation, managed volumes = create + flag for data copy
- [ ] Functions worker: get DDL from source, replay on target, record status
- [ ] Views worker: resolve dependency order via topological sort, replay DDLs in order on target
- [ ] Grants worker: collect SHOW GRANTS at catalog/schema level, replay GRANT statements on target. Runs last (all objects must exist)
- [ ] All workers write to migration_status with job_run_id, timing
- [ ] Commit

---

## Task 12: Migrate Workflow Definition

**Files:**
- Create: `uksouth_migration/tool/src/migrate/summary.py`
- Create: `uksouth_migration/tool/resources/migrate_workflow.yml`

- [ ] Summary notebook: aggregate migration_status, show totals, failures, DLT-skipped objects
- [ ] Workflow YAML with task ordering:
  - setup_sharing → orchestrator
  - orchestrator → parallel: for_each(managed), for_each(external), for_each(volumes), functions
  - managed + external + functions → views
  - views + volumes → grants
  - grants → summary
- [ ] Commit

---

## Task 13: Lakeview Dashboard

**Files:**
- Create: `uksouth_migration/tool/dashboards/migration_dashboard.lvdash.json`

- [ ] Create dashboard placeholder (4 pages: Overview, Detail, Validation, Pre-Checks)
- [ ] Full dashboard definition to be built after first migration run populates tracking tables
- [ ] Commit

---

## Task 14: Final Assembly and Validation

- [ ] Verify all __init__.py files exist
- [ ] Run `databricks bundle validate`
- [ ] Final commit

---

# Phase 2: Dashboard Polish + Hive Data Migration

**Goal:** Close out Phase 1 operator visibility (Lakeview dashboard smoke test against real migration runs), then add legacy Hive Metastore (`hive_metastore` catalog) migration as a new domain. The Hive workers **upgrade** Hive tables to UC on the target workspace rather than recreating them under `hive_metastore` — the new CP deployment is UC-first.

**Basis:** Sections 2.1 and 2.2 of the [Migration Guide](https://docs.google.com/document/d/1wf03gtFsIHKMpIMMBE3k4DyWKSfHg-jnM8iQGAihjpw/).

**Why Hive → UC upgrade (not Hive → Hive):** The customer-facing migration guide recommends upgrading to UC as part of the CP move rather than reproducing `hive_metastore` on target. Hive managed tables on DBFS root become UC managed; Hive managed non-DBFS and Hive external become UC external on target, pointing at the same customer ADLS.

**Architecture delta:**
- New workflow: `hive_migrate_workflow.yml`. Separate from `migrate_workflow` because discovery source is `hive_metastore` (not UC information_schema) and target mapping is configurable (which UC catalog on target receives each Hive database).
- New discovery tracking: `hive_discovery_inventory` schema is slightly different from UC — adds `storage_location` (raw path) and `data_category` (DBFS root / non-DBFS / external).
- New config parameter `hive_target_catalog` — where Hive databases get upgraded to on target (default: `hive_upgraded`).

### Hive Data Category → Migration Strategy (per Section 2.1 of the guide)

| Category | Source State | Target Strategy | Complexity |
|---|---|---|---|
| 1a. Hive managed, DBFS root | Data in source DBFS | **Data copy** (CTAS over Delta Sharing for Delta, CTAS over JDBC/file read for non-Delta) → UC managed on target | HIGH |
| 1b. Hive managed, non-DBFS (ADLS mount) | Data in customer ADLS, registered managed | Re-register as **UC external** on target pointing to same path | MEDIUM |
| 1c. Hive external | Data in customer ADLS | Re-register as **UC external** on target | LOW |
| 2a/2b. External Hive Metastore | Metadata in customer MySQL/Azure SQL | Customer reconfigures new workspace to same external metastore (no data migration needed) | LOW — doc only |

### Out of scope for Phase 2

- External Hive metastore (Section 2.2) migration — documented customer config, not code. Noted in Task 24.
- Hive materialized views and streaming tables — these don't exist in `hive_metastore`; only applicable to UC (covered in Phase 1).

---

## Task 15: Phase 1 Lakeview Dashboard Smoke Test

**Files:**
- Modify: `dashboards/migration_dashboard.lvdash.json` (only if issues surface)

- [ ] Deploy `migration_dashboard.lvdash.json` via `databricks bundle deploy` against `migration_tracking.cp_migration`
- [ ] Verify each panel renders correctly with data from the integration test run
- [ ] Check failure/warning filters, date-range, per-object-type breakdown
- [ ] Document operator onboarding path in README (bundle deploy → dashboard URL → how to read each page)
- [ ] Fix any panel SQL or visualisation issues
- [ ] Commit

---

## Task 16: Hive Discovery Extension

**Files:**
- Modify: `src/common/catalog_utils.py`
- Create: `src/discovery/hive_discovery.py`

- [ ] Add `list_hive_databases()` — `SHOW DATABASES IN hive_metastore`
- [ ] Add `classify_hive_tables(database)` — for each table in hive DB, read from `SHOW TABLES` + `DESCRIBE EXTENDED` to get: `table_type` (MANAGED / EXTERNAL / VIEW), `storage_location`, `data_source_format`, `provider`
- [ ] Add `categorize_hive_table(table_info)` — returns one of `hive_managed_dbfs_root`, `hive_managed_nondbfs`, `hive_external`, `hive_view`, `hive_function` based on `storage_location` pattern and type
- [ ] Create `hive_discovery.py` notebook mirroring UC discovery's structure — populates a new table `migration_tracking.cp_migration.hive_discovery_inventory`
- [ ] Add schema for `hive_discovery_inventory` to `tracking.py` — same fields as `discovery_inventory` plus `data_category` (STRING) and `storage_location` (STRING)
- [ ] Commit

---

## Task 17: Hive Pre-Check Extension

**Files:**
- Modify: `src/pre_check/pre_check.py`

- [ ] Add check: source workspace has `hive_metastore` catalog accessible
- [ ] Add check: target workspace has the configured `hive_target_catalog` (or permission to create it)
- [ ] Add check: for each distinct storage location in Hive inventory that's non-DBFS, verify the target workspace can read it (storage credential + external location exists on target)
- [ ] Add check: no Hive tables use features unsupported by UC (e.g. bucketing with incompatible bucket counts) — flag these as WARN with guidance
- [ ] Commit

---

## Task 18: Hive External Table Worker

**Files:**
- Create: `src/migrate/hive_external_worker.py`

Cleanest case: data already on customer ADLS, target workspace has access via same storage credential, just recreate the table DDL as UC external.

- [ ] Parse batch of hive_external records from orchestrator
- [ ] For each table: read DDL via `SHOW CREATE TABLE hive_metastore.{db}.{t}`, parse out `LOCATION`, columns, partitioning, table properties
- [ ] Rewrite as UC external DDL: `CREATE TABLE {hive_target_catalog}.{db}.{t} (...) USING {fmt} LOCATION '{same_path}'`
- [ ] Execute on target via statement execution API
- [ ] Validate row count matches
- [ ] Commit

---

## Task 19: Hive Managed Non-DBFS Worker

**Files:**
- Create: `src/migrate/hive_managed_nondbfs_worker.py`

Data on customer ADLS but registered as MANAGED in Hive. Target treats as UC external (data stays put, metadata re-registered).

- [ ] For each record: read Hive DDL + location
- [ ] Emit as UC external: `CREATE TABLE {hive_target_catalog}.{db}.{t} USING {fmt} LOCATION '{customer_adls_path}'`
- [ ] For non-Delta tables: run `MSCK REPAIR TABLE` to rebuild partition metadata
- [ ] Validate row count
- [ ] Commit

---

## Task 20: Hive Managed DBFS Root Worker

**Files:**
- Create: `src/migrate/hive_managed_dbfs_worker.py`

The complex case — data is in source-workspace DBFS root, **not** accessible from target workspace. Must copy.

**Approach:** Run a source-workspace-side pre-migration step that copies each Hive DBFS table to a staging external location (customer-provided ADLS path) via CTAS, then the worker on target registers the staging data as a UC managed table via DEEP CLONE (for Delta) or CREATE TABLE AS SELECT (for non-Delta) over the staging location.

- [ ] Add new workflow step `hive_stage_dbfs` (runs on source workspace cluster) — for each hive_managed_dbfs_root record, CTAS to `abfss://{staging_container}@{customer_account}/{db}/{table}/`
- [ ] Worker reads staged data and creates UC managed table on target: Delta → DEEP CLONE; non-Delta → CTAS over registered staging external location
- [ ] Requires new config param `hive_staging_location` (customer-provided ADLS path the SPN has write access to)
- [ ] Mark staging data with cleanup annotation; teardown step removes staging blobs after successful migration
- [ ] Validate row count
- [ ] Commit

---

## Task 21: Hive Views Worker

**Files:**
- Create: `src/migrate/hive_views_worker.py`

Hive view definitions reference `hive_metastore.{db}.{table}`. On target, those references must be rewritten to `{hive_target_catalog}.{db}.{table}`.

- [ ] Parse view DDLs
- [ ] Apply namespace rewriting: `hive_metastore.` → `{hive_target_catalog}.`
- [ ] Topological sort by view-to-view dependencies (already-migrated views may be referenced)
- [ ] Execute rewritten DDL on target
- [ ] Commit

---

## Task 22: Hive Functions Worker

**Files:**
- Create: `src/migrate/hive_functions_worker.py`

- [ ] Discover Hive UDFs via `SHOW USER FUNCTIONS IN hive_metastore.{db}`
- [ ] For each function, extract DDL via `DESCRIBE FUNCTION EXTENDED`
- [ ] Rewrite namespace + any `hive_metastore.` references in the body
- [ ] Recreate on `{hive_target_catalog}.{db}.{function}`
- [ ] Commit

---

## Task 23: Hive Grants / ACLs Worker

**Files:**
- Create: `src/migrate/hive_grants_worker.py`

Legacy Hive uses `databricks_sql_permissions` with `GRANT SELECT/MODIFY/...` syntax. Map to UC grants — the privilege names differ (e.g. Hive MODIFY ≈ UC MODIFY on tables; Hive ALL PRIVILEGES expands to UC equivalents).

- [ ] Discover Hive grants via `SHOW GRANTS ON hive_metastore` at catalog/schema/table levels
- [ ] Map Hive privileges to UC privileges using a small translation table (document any unmappable ones)
- [ ] Emit UC `GRANT` statements against `{hive_target_catalog}`
- [ ] Commit

---

## Task 24: External Hive Metastore Documentation

**Files:**
- Create: `docs/external_hive_metastore.md`

Not code — customer configuration. Document the steps to reconnect a new workspace to the same external MySQL/Azure SQL metastore.

- [ ] Document required cluster init scripts for external metastore connection
- [ ] Document Spark confs (`javax.jdo.option.*`)
- [ ] Document credential re-provisioning (the new workspace's SPN needs DB access)
- [ ] Document validation: `SHOW DATABASES` on new workspace should return same list as old
- [ ] Commit

---

## Task 25: Hive Migration Workflow Definition

**Files:**
- Create: `resources/hive_migrate_workflow.yml`

Separate workflow from the UC `migrate_workflow`. Dependency ordering:

```
hive_stage_dbfs (source-side CTAS to staging)
   │
   ▼
hive_orchestrator
   │
   ▼ parallel for_each
   ├── hive_external_worker
   ├── hive_managed_nondbfs_worker
   └── hive_managed_dbfs_worker (reads from staging)
   │
   ▼ (after tables)
   ├── hive_functions_worker
   └── hive_views_worker
   │
   ▼ (after views)
   hive_grants_worker
   │
   ▼
summary (shared)
```

- [ ] Define YAML mirroring migrate_workflow structure
- [ ] Share `summary.py` with UC migrate — extend to aggregate both domains
- [ ] Commit

---

## Task 26: Hive Integration Test Coverage

**Files:**
- Modify: `tests/integration/seed_test_data.py`
- Create: `tests/integration/test_hive_end_to_end.py` (or extend existing test_end_to_end.py)
- Modify: `resources/integration_test_workflow.yml`

- [ ] Seed: `CREATE DATABASE hive_metastore.test_hive`; add one external Delta table on a test ADLS path, one managed non-DBFS, one view, one function
- [ ] Skip DBFS-root managed table case in integration test (requires real DBFS data and is slow); cover via unit test only
- [ ] Extend integration workflow to call `hive_migrate_workflow` after UC `migrate_workflow`
- [ ] Assertions: target `hive_target_catalog.test_hive.*` objects exist with expected row counts
- [ ] Commit

---

# Phase 3: Extended UC Object Coverage

**Goal:** Extend the tool from the core UC object types (catalogs, schemas, managed/external tables, volumes, views, functions, grants) to the full UC governance surface — tags, ABAC policies, row filters, column masks, monitors, registered models, connections, foreign catalogs, online tables, Delta Sharing objects, and comments/table properties.

**Basis:** [`docs/terraform_exporter_analysis.md`](../terraform_exporter_analysis.md) — decision to migrate all UC objects via code rather than split between Terraform exporter and code.

**Architecture delta:**
- No schema change to the 3 tracking tables — `object_type` is a free-form STRING column; new values slot in.
- Extended discovery surface: REST endpoints for objects not exposed via `information_schema` (policies, monitors, models, connections, online tables).
- 11 new workers in the migrate workflow, grouped into four dependency tiers after the existing `grants` step.

### New Task Chain (after Task 12's migrate workflow)

```
                 (existing)
tables ───► functions ───► views ───► grants
                                         │
                                         ▼
  Tier A — table-level governance:
      tags_worker, row_filters_worker, column_masks_worker, policies_worker
      (policies depend on tags; row_filters/column_masks depend on functions)
                                         │
                                         ▼
  Tier B — object governance & metadata:
      comments_properties_worker (non-Delta only — Delta inherits from DEEP CLONE)
                                         │
                                         ▼
  Tier C — independent object types:
      monitors_worker, models_worker, connections_worker, foreign_catalogs_worker
      (foreign_catalogs depends on connections)
                                         │
                                         ▼
  Tier D — sharing & derived:
      sharing_worker, online_tables_worker
      (online_tables depends on source managed tables existing on target)
```

### Out of scope (per migration guide)

- **Data Classification** — auto-rebuilds on the new metastore.
- **Lineage** — system-generated, rebuilds as jobs run.
- **Audit logs** — one-off export recommended before decommission, not part of recurring tool.

---

## Task 27: Discovery Extensions for New Object Types

**Files:**
- Modify: `src/common/catalog_utils.py` — add list/read helpers for new objects
- Modify: `src/discovery/discovery.py` — extend to discover and record new object types

- [ ] Add `list_tags()` — read `system.information_schema.{catalog,schema,table,column,volume}_tags`, return normalized records with `securable_type`, `securable_fqn`, `tag_name`, `tag_value`
- [ ] Add `list_row_filters()` — `information_schema.tables WHERE row_filter IS NOT NULL`, return `table_fqn`, `filter_function_fqn`, `filter_columns`
- [ ] Add `list_column_masks()` — `information_schema.columns WHERE column_mask IS NOT NULL`, return `table_fqn`, `column_name`, `mask_function_fqn`
- [ ] Add `list_policies()` — REST `GET /api/2.1/unity-catalog/policies` via `source_client.api_client`; enumerate ABAC policy definitions
- [ ] Add `list_monitors()` — iterate discovered tables, call REST `GET /api/2.1/unity-catalog/tables/{name}/monitor`; skip 404s
- [ ] Add `list_registered_models()` — SDK `source_client.registered_models.list()` per schema; include artifact location
- [ ] Add `list_connections()` — SDK `source_client.connections.list()`
- [ ] Add `list_foreign_catalogs()` — `source_client.catalogs.list(include_browse=True)` and filter where `catalog_type == "FOREIGN_CATALOG"`
- [ ] Add `list_online_tables()` — REST `GET /api/2.0/online-tables`; each row includes source table FQN, primary keys, timeseries key
- [ ] Add `list_shares()`, `list_recipients()`, `list_providers()` — SDK calls on source workspace; EXCLUDE the migration share `cp_migration_share` and its recipient
- [ ] Update `discovery.py` to invoke all of the above, write to `discovery_inventory` with appropriate `object_type` values (`tag`, `row_filter`, `column_mask`, `policy`, `monitor`, `registered_model`, `connection`, `foreign_catalog`, `online_table`, `share`, `recipient`, `provider`)
- [ ] Commit

---

## Task 28: Tags Worker

**Files:**
- Create: `src/migrate/tags_worker.py`

Tags are set per-securable via `ALTER <SECURABLE> SET TAGS (key = value, ...)`. Tags on columns use `ALTER TABLE t ALTER COLUMN c SET TAGS (...)`.

- [ ] Parse `tag` records from `orchestrator` task value
- [ ] Group by `securable_fqn` + `securable_type` so all tags for one object apply in a single statement
- [ ] Build and execute SQL:
  - Catalog/schema: `ALTER {type} {fqn} SET TAGS ('k' = 'v', ...)`
  - Table: `ALTER TABLE {fqn} SET TAGS ('k' = 'v', ...)`
  - Column: `ALTER TABLE {fqn} ALTER COLUMN {col} SET TAGS ('k' = 'v', ...)`
  - Volume: `ALTER VOLUME {fqn} SET TAGS ('k' = 'v', ...)`
- [ ] Idempotent: DESCRIBE EXTENDED on target first, skip tags already present
- [ ] Record status per tag-assignment in `migration_status`
- [ ] Commit

---

## Task 29: Row Filters Worker

**Files:**
- Create: `src/migrate/row_filters_worker.py`

Row filter functions must already exist on target (migrated by `functions_worker`). If the referenced function is missing, record as `failed` with a clear error.

- [ ] For each row_filter record, look up the referenced UDF on target via information_schema
- [ ] Execute: `ALTER TABLE {table_fqn} SET ROW FILTER {function_fqn} ON ({columns})`
- [ ] Validate: `DESCRIBE EXTENDED` shows the filter is applied
- [ ] Commit

---

## Task 30: Column Masks Worker

**Files:**
- Create: `src/migrate/column_masks_worker.py`

Same pattern as row_filters — depends on function migration.

- [ ] For each column_mask record, check target function exists
- [ ] Execute: `ALTER TABLE {table_fqn} ALTER COLUMN {col} SET MASK {function_fqn}`
- [ ] Validate via `information_schema.column_masks`
- [ ] Commit

---

## Task 31: ABAC Policies Worker

**Files:**
- Create: `src/migrate/policies_worker.py`

Policies reference tags, so run AFTER `tags_worker`. Use REST API `POST /api/2.1/unity-catalog/policies` per policy. Validate idempotency by matching on `name`.

- [ ] For each policy record, check if policy with same `name` already exists on target; skip if present
- [ ] POST the policy definition (comment, rule, on_securable, exclude, when, match_columns, etc.)
- [ ] Record status
- [ ] Commit

---

## Task 32: Comments & Table Properties Worker (non-Delta only)

**Files:**
- Create: `src/migrate/comments_properties_worker.py`

Delta DEEP CLONE (in `managed_table_worker`) preserves comments and TBLPROPERTIES automatically. This worker runs only for non-Delta managed tables and all external tables.

- [ ] Filter discovery to non-Delta tables
- [ ] For each: emit `COMMENT ON TABLE ... IS '...'`, `COMMENT ON COLUMN ... IS '...'` from `information_schema.columns.comment`
- [ ] Emit `ALTER TABLE ... SET TBLPROPERTIES (...)` for non-reserved properties
- [ ] Also emit `COMMENT ON CATALOG` / `COMMENT ON SCHEMA` for all catalogs/schemas (Delta CLONE doesn't cover parent objects)
- [ ] Commit

---

## Task 33: Lakehouse Monitors Worker

**Files:**
- Create: `src/migrate/monitors_worker.py`

Monitor definitions are per-table. Target table must exist. Metric history does NOT transfer — monitoring restarts from scratch.

- [ ] For each monitor record, POST to `/api/2.1/unity-catalog/tables/{name}/monitor` on target
- [ ] Preserve: `schedule`, `inference_log`, `snapshot`, `time_series`, `baseline_table_name`, `output_schema_name`, `slicing_exprs`, `custom_metrics`, `data_classification_config`, `notifications`
- [ ] Record status + note in `error_message` that metric history was not transferred
- [ ] Commit

---

## Task 34: Registered Models Worker (metadata + artifacts)

**Files:**
- Create: `src/migrate/models_worker.py`

Two-phase: (a) model metadata via SDK, (b) artifact copy via `dbutils.fs.cp` or `databricks fs cp` between UC storage accounts. Flag as **Phase 2b — larger effort**; can be shipped after the other workers if customer need is deferred.

- [ ] Create the registered model shell on target via `target_client.registered_models.create`
- [ ] For each version:
  - [ ] Copy model artifacts from source-metastore storage to target-metastore storage (ABFSS → ABFSS)
  - [ ] Register the version via `target_client.model_versions.create` pointing at the new source URI
  - [ ] Copy tags and aliases on the version
- [ ] Handle model signatures, requirements files, logged metrics
- [ ] Record per-version status
- [ ] Commit

---

## Task 35: Connections + Foreign Catalogs Worker

**Files:**
- Create: `src/migrate/connections_worker.py`

Passwords in `options` are not returned by the GET API — the tool will create the connection shell, record status `partial`, and surface the list of credentials the customer must re-enter.

- [ ] For each connection: `target_client.connections.create(name, connection_type, options, comment)` — with `options` containing only non-sensitive fields returned by GET
- [ ] Append `needs_manual_reentry=true` to `error_message` for tracking-table visibility
- [ ] For each foreign_catalog record: `target_client.catalogs.create(name=..., connection_name=..., options=...)` — depends on the connection existing
- [ ] Record status
- [ ] Commit

---

## Task 36: Online Tables Worker

**Files:**
- Create: `src/migrate/online_tables_worker.py`

Online tables are serving-layer projections of UC tables. Creation triggers a full re-sync from the source Delta table (which must be migrated first).

- [ ] For each online_table record, POST `/api/2.0/online-tables` on target with source table FQN, primary key columns, timeseries key, run triggered/continuous policy
- [ ] Do NOT wait for sync completion — record status `provisioned`, surface re-sync duration in notes
- [ ] Commit

---

## Task 37: Delta Sharing Objects Worker

**Files:**
- Create: `src/migrate/sharing_worker.py`

This migrates the customer's **own** Delta Sharing configuration (their shares/recipients/providers) — separate from the migration's internal `cp_migration_share` which is set up by `setup_sharing.py`.

- [ ] For each share (excluding `cp_migration_share`): recreate on target via `target_client.shares.create`, then add objects via `target_client.shares.update`
- [ ] For each recipient: `target_client.recipients.create` — record that activation tokens will be new, customer must redistribute
- [ ] For each provider: `target_client.providers.create` pointing at the same provider URL
- [ ] Record status + surface "new activation tokens" in notes
- [ ] Commit

---

## Task 38: Workflow Definition Update

**Files:**
- Modify: `resources/migrate_workflow.yml`

Wire the new tasks into the DAG with the Tier A→D ordering.

- [ ] Add 11 new tasks to `migrate_workflow.yml` with `depends_on` per the ordering
- [ ] Tier A (tags, row_filters, column_masks, policies) depends on `migrate_grants`
- [ ] Tier B (comments_properties) depends on `migrate_grants`; parallel with Tier A
- [ ] Tier C (monitors, models, connections → foreign_catalogs) depends on `migrate_grants`; parallel with Tier A/B
- [ ] Tier D (sharing, online_tables) depends on managed/external tables having succeeded
- [ ] `summary` task now depends on all Tier D tasks (was: `migrate_grants`)
- [ ] Integration test workflow updated to invoke the full extended migrate job
- [ ] Commit

---

## Task 39: Dashboard Extensions

**Files:**
- Modify: `dashboards/migration_dashboard.lvdash.json`

- [ ] Add per-object-type breakdown rows for the 11 new types
- [ ] Add a dedicated page for governance objects (tags, policies, filters, masks) with success/failure counts
- [ ] Add a "requires manual re-entry" filter (surfaces connections with redacted secrets, recipients with new activation tokens)
- [ ] Commit

---

## Task 40: Integration Test Coverage

**Files:**
- Modify: `tests/integration/seed_test_data.py` — seed tag assignments, row filter, column mask, monitor, connection, model
- Modify: `tests/integration/test_end_to_end.py` — assert new object types appear in `migration_status` as `validated`

- [ ] Seed: add a UDF, apply it as row filter on `managed_orders`, apply as column mask on `amount`
- [ ] Seed: apply tags to catalog, schema, `managed_orders`, and its `amount` column
- [ ] Seed: create a connection (to a public endpoint like HTTPBin or a no-op PostgreSQL) and a foreign catalog from it
- [ ] Seed: create a Lakehouse Monitor on `managed_orders` (snapshot profile for speed)
- [ ] `test_end_to_end`: assert counts per new object type match source
- [ ] Commit

---

## Task 41: Phase 3 Final Validation

- [ ] Full integration run on serverless, all 28 tasks green
- [ ] Dashboard renders all new object types correctly
- [ ] Document per-object-type operator notes (e.g., "connections require password re-entry post-migration") in README
- [ ] Final commit
