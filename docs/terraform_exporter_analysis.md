# Terraform Exporter Analysis for UK South Migration

## Decision Summary (2026-04-18)

**UC objects → Code (this DAB tool). Non-UC workspace assets → Terraform Exporter.**

We evaluated the exporter against the 16 UC-governance objects enumerated in Section 2.3.5 of the [Migration Guide](https://docs.google.com/document/d/1wf03gtFsIHKMpIMMBE3k4DyWKSfHg-jnM8iQGAihjpw/). The exporter cleanly covers only 3 of them (Foreign Catalogs, Providers, Lakehouse Monitors) and is materially incomplete for the rest — most notably ABAC policies, row filters, column masks, and per-entity tag assignments, which would silently drop during a TF-based migration.

Because splitting 16 UC object types between two tools (TF for 3, code for 13) creates two reconciliation passes and an opaque customer workflow, we migrate **all UC objects via code** and reserve the exporter for workspace assets (Phase 3 of the guide: notebooks, jobs, cluster policies, SQL warehouses, dashboards, secrets, identity, etc.).

### UC Object Coverage Matrix

| UC Object | TF Resource | Exporter? | Decision | Rationale |
|---|---|---|---|---|
| Catalogs, Schemas | Yes | Yes | **Code** | Already in the tool |
| Managed tables | Yes | Metadata only | **Code** | Data copy needed (DEEP CLONE) — already in tool |
| External tables | Yes | Metadata only | **Code** | Already in tool |
| Volumes (ext / managed) | Yes | Yes / partial | **Code** | Already in tool |
| Views, Functions | Yes | Yes | **Code** | Already in tool; dependency ordering matters |
| Grants | Yes | Yes, diffs | **Code** | Already in tool |
| Registered Models | Yes | Metadata only | **Code** | Artifact copy via REST; exporter can't do it |
| Connections (Fed) | Yes | Yes, no passwords | **Code** | Same password gap both ways; keeps tool coherent |
| Foreign Catalogs | Yes | Yes | **Code** | Keeps all UC in one tool |
| Shares / Recipients / Providers | Yes | Yes | **Code** | Customer's own shares (ours is handled separately by setup_sharing) |
| Online Tables | `databricks_online_table` | Not in exporter | **Code** | REST only |
| Governed Tags | `databricks_entity_tag_assignment` | Only `tag_policy` definitions | **Code** | Assignments dropped by exporter |
| ABAC Policies | `databricks_policy_info` (PrPr) | No | **Code** | REST `/unity-catalog/policies` |
| Row Filters | Via policy_info only | No | **Code** | SQL `ALTER TABLE ... SET ROW FILTER` |
| Column Masks | Via policy_info only | No | **Code** | SQL `ALTER ... SET MASK` |
| Comments | Partial (on parent) | Partial | **Code** | Preserved by DEEP CLONE for Delta; DDL for rest |
| Table Properties | Yes | Yes | **Code** | Preserved by DEEP CLONE for Delta; DDL for rest |
| Lakehouse Monitors | `data_quality_monitor` | Yes (`dq`) | **Code** | Only to keep UC in one tool; TF would also work |
| Data Classification | N/A | N/A | **Out of scope** | Auto-rebuilds on new metastore |
| Lineage | N/A | N/A | **Out of scope** | Auto-rebuilds as jobs run |

## Overview

The Databricks Terraform Exporter is an **experimental** tool built into the `terraform-provider-databricks` binary. It exports workspace and account-level resources as Terraform HCL + state files, enabling recreation on a new workspace.

**Scope for this project:** Per the decision above, we use the exporter **only for non-UC workspace assets** (Phase 3 of the migration guide). The content below describes the exporter's full capability surface and is retained for the workspace-assets implementation.

**Key takeaway for workspace assets**: The exporter handles ~80% of workspace configuration migration but does NOT handle data migration, MLflow experiments, or secret values.

## What the Exporter CAN Migrate

### Fully Supported — High Confidence

| Category | Resources | Notes |
|----------|-----------|-------|
| **Notebooks & Files** | `databricks_notebook`, `databricks_workspace_file`, `databricks_directory` | Content exported locally. Supports incremental. 10MB file size limit. |
| **Git Repos** | `databricks_repo` | Exports repo config (URL, branch). Code stays in Git. |
| **Jobs** | `databricks_job` | Full multi-task support. Auto-discovers dependencies (notebooks, clusters, warehouses). **DABs-managed jobs excluded.** |
| **DLT Pipelines** | `databricks_pipeline` | Exports pipeline definition. Incremental support. **Does NOT migrate checkpoint state** — pipelines must do full refresh on new workspace. |
| **Cluster Policies** | `databricks_cluster_policy` | Policy IDs change. All job references auto-updated in HCL. |
| **Clusters** | `databricks_cluster` | Config exported. Libraries included inline. |
| **Instance Pools** | `databricks_instance_pool` | Straightforward export/import. |
| **SQL Warehouses** | `databricks_sql_endpoint` | Config exported. IDs change. |
| **SQL Queries** | `databricks_query` | Incremental support. |
| **Lakeview Dashboards** | `databricks_dashboard` | JSON definition exported. |
| **Legacy SQL Dashboards** | `databricks_sql_dashboard`, `databricks_sql_widget`, `databricks_sql_visualization` | Full export of dashboard + widgets + visualizations. |
| **Alerts** | `databricks_alert`, `databricks_alert_v2` | Incremental support. |
| **Model Serving Endpoints** | `databricks_model_serving` | Exports endpoint config. **Does NOT migrate serving state** — endpoint must be redeployed. |
| **Vector Search** | `databricks_vector_search_endpoint`, `databricks_vector_search_index` | Exports config. **Does NOT migrate index data** — must re-index from source. |
| **Apps** | `databricks_app` | Exports app definition. Must redeploy. |
| **Lakebase** | `databricks_database_instance` | Exports instance config. |
| **Secret Scopes** | `databricks_secret_scope`, `databricks_secret`, `databricks_secret_acl` | Scopes and ACLs exported. **Values redacted** — use `-export-secrets` flag to write values to `terraform.tfvars`. |
| **Permissions** | `databricks_permissions` | Auto-emitted when parent objects are exported. |
| **IP Access Lists** | `databricks_ip_access_list` | Incremental support. |
| **Global Init Scripts** | `databricks_global_init_script` | Incremental support. |
| **Workspace Config** | `databricks_workspace_conf` | Partial support. |

### Unity Catalog — Metadata Only (No Data)

| Resource | Notes |
|----------|-------|
| `databricks_catalog` | Recreates catalog definition |
| `databricks_schema` | Recreates schema definition |
| `databricks_sql_table` | Exports table DDL. **Does NOT copy data.** External tables: just metadata. Managed tables: creates empty table — data must be separately migrated via DEEP CLONE. |
| `databricks_volume` | External volumes: metadata only. Managed volumes: config only — data must be copied separately. |
| `databricks_external_location` | Points to same ADLS path |
| `databricks_storage_credential` | **Sensitive fields (passwords/keys) NOT exported** — must be re-provisioned |
| `databricks_connection` | **Sensitive fields missing from API response** |
| `databricks_grants` | Known issue: import may show plan diffs. Requires careful validation. |
| `databricks_registered_model` (UC) | Exports model metadata. **Does NOT copy model artifacts.** |
| `databricks_share`, `databricks_recipient`, `databricks_provider` | Delta Sharing config. Recipients get new activation links. |
| `databricks_online_table` | Config only. Must re-sync. |
| `databricks_data_quality_monitor` | Config only. |

### Identity / SCIM

| Resource | Notes |
|----------|-------|
| `databricks_user` | Workspace + Account level. With Identity Federation, exported as data sources (not resources). |
| `databricks_group`, `databricks_group_member` | Account groups persist. Workspace-local groups need recreation. |
| `databricks_service_principal` | Account-level SPs exported. SP application IDs may differ across accounts. |
| `databricks_mws_permission_assignment` | Workspace-to-user/group assignments at account level. |

### Account-Level Infrastructure

| Resource | Notes |
|----------|-------|
| `databricks_mws_networks` | Network config |
| `databricks_mws_private_access_settings` | Private access config |
| `databricks_mws_vpc_endpoint` | VPC/PE config |
| `databricks_mws_customer_managed_keys` | CMK config |
| `databricks_mws_workspaces` | Workspace definitions |

## What the Exporter CANNOT Migrate

### Not Supported at All

| Asset | Why | Alternative |
|-------|-----|-------------|
| **MLflow Experiments** | Not implemented in exporter. API gaps for serialization. | Use **`mlflow-export-import`** tool (open source from Databricks Labs) |
| **MLflow Models (workspace registry)** | Not implemented. | Use **`mlflow-export-import`** tool |
| **MLflow Runs & Artifacts** | Not exportable. | Use **`mlflow-export-import`** or manual artifact copy |
| **Table Data (managed tables)** | Exporter only handles DDL, not data. | **DEEP CLONE** or **CTAS** via SQL scripts |
| **Volume Data (managed volumes)** | Config only. | **AzCopy** or **databricks fs cp** |
| **DBFS Root Data** | Not systematically exported. | **AzCopy** between storage accounts |
| **Job Run History** | Control plane internal data. | Export audit logs beforehand if needed |
| **Query History** | Control plane internal data. | Export via system tables if available |
| **Notebook Revision History** | Not preserved on export/import. | Accept loss or use Git for versioning |
| **Notebook Comments** | Not exported. | Accept loss |
| **SQL Permissions (legacy)** | `databricks_sql_permissions` not supported. | Manual recreation or migrate to UC grants |
| **User Tokens (PATs)** | Cannot be exported. | Users must regenerate tokens on new workspace |
| **Git Credentials** | `databricks_git_credential` not explicitly exportable. | Users must re-authenticate Git on new workspace |
| **Streaming Checkpoints** | Internal state, not a Terraform resource. | Manual handling — copy from ADLS or restart from offset |
| **DLT Pipeline State** | Checkpoint/event log is workspace-scoped. | Full refresh on new workspace |
| **Serving Endpoint State** | Traffic routing, A/B config tied to workspace. | Redeploy model, re-apply traffic config |
| **Vector Search Index Data** | Index data is internal. | Re-index from source Delta table |
| **DABs-Managed Jobs** | Explicitly excluded from export. | Redeploy via DABs CLI to new workspace |

### Partially Supported (Requires Manual Intervention)

| Asset | Issue | Workaround |
|-------|-------|------------|
| **Secret Values** | Redacted by default. | Use `-export-secrets` flag → generates `terraform.tfvars` with actual values. Handle securely. |
| **Storage Credentials** | Sensitive fields (keys, passwords) not returned by API. | Customer must re-provision credentials on new workspace. |
| **UC Connections** | `options` block incomplete (passwords omitted). | Customer must re-enter connection passwords. |
| **UC Grants** | Import shows plan diffs (GitHub #3245). | Validate plan, may need manual adjustments. |
| **Identity Federation Users** | Account-level groups exported as workspace groups when applied. | Use account-level export separately, then assign to new workspace. |
| **Large Workspaces** | API rate limit (~30 req/s) makes export very slow. State refresh can take hours. | Use service filtering (`-listing`) to export in batches. |

## Recommended Exporter Workflow for UK South Migration

### Step 1: Export from Old Workspace

```bash
export DATABRICKS_HOST="https://<old-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# Full workspace export (excluding UC data — handle separately)
terraform-provider-databricks exporter -skip-interactive \
  -directory=./export_output \
  -listing=jobs,compute,notebooks,wsfiles,repos,policies,pools,secrets,access,dlt,model-serving,vector-search,dashboards,queries,alerts,sql-endpoints,sql-dashboards,groups,users \
  -services=all,-uc-tables,-uc-schemas,-uc-volumes \
  -export-secrets \
  -notebooksFormat=SOURCE \
  -debug
```

### Step 2: Export UC Metadata (Separate Pass)

```bash
# UC metadata export (catalogs, schemas, table DDL, locations, credentials, grants)
terraform-provider-databricks exporter -skip-interactive \
  -directory=./export_uc \
  -listing=uc-catalogs,uc-external-locations,uc-storage-credentials,uc-grants \
  -services=uc \
  -debug
```

### Step 3: Modify for New Workspace

- Update provider block to point to new workspace URL
- Replace any hardcoded workspace IDs
- Update storage credential configs for new managed identity
- Review `terraform.tfvars` for secret values

### Step 4: Apply to New Workspace

```bash
export DATABRICKS_HOST="https://<new-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

cd ./export_output
terraform init
terraform plan    # Review carefully
terraform apply
```

### Step 5: Migrate Data Separately

```bash
# For each managed UC table — run on new workspace
# DEEP CLONE from external location or cross-workspace reference
CREATE TABLE new_catalog.schema.table DEEP CLONE old_catalog.schema.table;

# For DBFS data
azcopy copy "https://<old-storage>.blob.core.windows.net/dbfs" \
             "https://<new-storage>.blob.core.windows.net/dbfs" --recursive
```

### Step 6: Handle MLflow Separately

```bash
# Use mlflow-export-import tool
pip install mlflow-export-import

# Export experiments
export-experiments --experiments "experiment1,experiment2" --output-dir ./mlflow_export

# Import to new workspace
import-experiments --input-dir ./mlflow_export
```

## Migration Coverage Summary

| Migration Category | Exporter Coverage | Gap Filler |
|-------------------|-------------------|------------|
| **Workspace Config** (notebooks, repos, files) | ~95% | Manual: revision history, comments, git credentials |
| **Compute** (clusters, policies, pools, warehouses) | ~100% | None |
| **Jobs & Orchestration** | ~90% | Gap: DABs-managed jobs (redeploy via DABs), run history (lost) |
| **SQL & Dashboards** | ~95% | Gap: query history (lost) |
| **DLT Pipelines** | ~80% (config only) | Gap: pipeline state, checkpoints → full refresh required |
| **ML Serving** | ~70% (config only) | Gap: serving state → redeploy model, update client URLs |
| **Vector Search** | ~70% (config only) | Gap: index data → re-index from source |
| **UC Metadata** | ~75% | Gap: grants import issues, sensitive credential fields, data migration |
| **UC Data** | 0% | DEEP CLONE / CTAS scripts required |
| **Secrets** | ~80% (with -export-secrets) | Gap: customer must verify all values transferred correctly |
| **Identity** | ~85% | Gap: IdFed complications, PATs must be regenerated |
| **MLflow** | 0% | mlflow-export-import tool required |
| **Streaming State** | 0% | Manual checkpoint migration or restart from offset |
| **DBFS Data** | ~10% (only referenced files) | AzCopy for bulk DBFS migration |
| **External Integrations** | 0% | Customer must update all CI/CD, BI tools, API clients manually |
