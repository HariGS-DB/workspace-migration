# UC Data Migration Tool — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DAB-packaged tool that migrates Unity Catalog data (managed tables, external tables, views, functions, volumes, grants) between Azure Databricks workspaces during control plane migrations, with pre-check validation, discovery, parallel batched migration, inline validation, and a Lakeview dashboard.

**Architecture:** Three Databricks Workflows (pre-check, discovery, migrate) backed by three Delta tracking tables. The migrate workflow uses `for_each_task` for parallel batched execution, with separate task chains per object type. Delta Sharing enables managed table migration via DEEP CLONE. SPN with OAuth secret authenticates to both source and target workspaces.

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
│       ├── setup_sharing.py                # Create shares, recipients, target catalogs/schemas
│       ├── orchestrator.py                 # Build batches per object type, output task values
│       ├── managed_table_worker.py         # DEEP CLONE managed tables via delta sharing
│       ├── external_table_worker.py        # Recreate external table metadata on target
│       ├── volume_worker.py                # Migrate volumes (external: metadata, managed: data copy)
│       ├── functions_worker.py             # Replay function DDLs on target
│       ├── views_worker.py                 # Replay view DDLs in dependency order on target
│       ├── grants_worker.py                # Replay GRANT statements on target
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
