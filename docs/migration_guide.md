# UK South Control Plane Migration Guide

## Context

Azure UK South workspaces currently use UK West as the control plane. Databricks is introducing a **dedicated UK South control plane**. This is NOT an in-place migration — customers must create new workspaces and UC metastores on the UK South CP, then migrate assets.

**Key constraints:**
- **Workspace IDs change** — all external references must be updated
- **New UC metastore required** — catalogs, schemas, tables, permissions must be migrated
- **New DBFS root** — fresh storage provisioned; anything in `dbfs:/` must be explicitly copied
- **New Private Endpoints** — customers must create new Azure Private Link endpoints
- **Data plane storage (ADLS Gen2) stays** — external tables and UC external locations point to the same storage

---

## 1. Customer Base Analysis (UK South)

**Q4 2025 (Oct–Dec) metrics from Logfood PUM, paying workspaces only:**

| Metric | Value |
|--------|-------|
| Total Paying Workspaces | 7,076 (across 1,222 accounts) |
| Q4 DBU Consumption | 56,936,634 DBUs |
| Q4 Dollar Consumption | $15,176,004 |

### Product Line Breakdown

| Product Line | Q4 DBUs | % | Q4 Dollars | Accounts | Workspaces |
|-------------|---------|---|------------|----------|------------|
| JOBS | 18,670,153 | 32.8% | $3,232,475 | 638 | 2,247 |
| ALL_PURPOSE | 14,833,243 | 26.1% | $5,343,005 | 1,042 | 5,507 |
| SQL | 9,544,910 | 16.8% | $4,744,379 | 813 | 3,505 |
| NETWORKING | 5,873,742 | 10.3% | $10,724 | 858 | 4,002 |
| MODEL_SERVING | 2,705,575 | 4.8% | $159,565 | 142 | 249 |
| DLT | 2,216,918 | 3.9% | $716,733 | 303 | 792 |
| VECTOR_SEARCH | 1,299,165 | 2.3% | $82,524 | 79 | 110 |
| INTERACTIVE (Serverless) | 574,340 | 1.0% | $412,094 | 682 | 2,991 |
| APPS | 372,861 | 0.7% | $253,270 | 162 | 369 |
| Background Governance* | 294,010 | 0.5% | $104,257 | 574+ | 1,782+ |
| DATABASE (Lakebase) | 230,813 | 0.4% | $38,093 | 91 | 122 |
| LAKEFLOW_CONNECT | 162,469 | 0.3% | $62,824 | 115 | 173 |
| FOUNDATION_MODEL_API | 133,004 | 0.2% | $11,080 | 41 | 61 |
| AI Governance** | 21,951 | <0.1% | $1,293 | 113+ | 187+ |
| DATA_SHARING | 3,434 | <0.1% | $1,377 | 13 | 18 |
| CLEAN_ROOM | 68 | <0.1% | $2,312 | 2 | 2 |

*Background Governance: Predictive Optimization, Lakehouse Monitoring, Data Classification, Fine-Grained Access Control, Data Quality Monitoring, Base Environments.
**AI Governance: AI Gateway, Agent Evaluation.

### UC vs Legacy Hive Adoption

| Metric | Value |
|--------|-------|
| UC Dollars | $8,494,734 (56.0%) |
| Non-UC (Legacy Hive) Dollars | $6,681,270 (44.0%) |

**Account distribution by UC adoption:**
- 100% UC: 2 accounts ($6)
- 75–99% UC: 129 accounts ($5,625,227 — 37.1%)
- 25–74% UC (mixed): 416 accounts ($6,143,470 — 40.5%)
- 1–24% UC (low): 218 accounts ($2,444,936 — 16.1%)
- 0% UC (legacy only): 443 accounts ($962,366 — 6.3%)

**UC Table Inventory (az-ukwest TiDB shard):** 12.6M objects total — 4.5M managed tables, 2.4M external tables, 4.3M views, 1.1M foreign/federated, 218K streaming/MVs/clones.

**Key accounts on legacy Hive:** Centrica PLC (15% UC, $634K), Adobe Systems (0% UC, $150K), Network Rail (24% UC, $316K), Estee Lauder (19% UC, $108K), ClearBank (20% UC, $101K).

---

## 2. Account Console & Identity Setup

Before migrating workspace assets, the account-level configuration must be set up correctly.

### Account Console Changes

| Action | Details |
|--------|---------|
| **Create new workspace** | Account Console → Workspaces → Create. Select UK South region. Configure VNet, subnets, NSG for data plane. |
| **Create new UC metastore** | Account Console → Catalog → Create Metastore. Region: UK South. Assign ADLS Gen2 container as root storage. |
| **Assign metastore to workspace** | Account Console → Catalog → Metastore → Workspace assignments. Bind new metastore to new workspace. |
| **Configure metastore admin** | Assign metastore admin group/user in Account Console. This identity manages catalogs, external locations, and grants. |
| **Network configuration** | Account Console → Networking (if using account-level network config). Create/update NCC (Network Connectivity Config) for new workspace. Add private endpoint rules. |
| **Private Access Settings** | Create new Private Access Settings for UK South CP. Associate with new workspace. |
| **Customer-Managed Keys** | If customer uses CMK: register new key vault/key in Account Console → Encryption. Assign to new workspace for managed services and/or DBFS. |
| **Budget policies** | Review and update budget policies to include new workspace. |
| **IP access lists** | If using account-level IP access lists, add new workspace. |

### Identity & Access Setup

| Step | Details |
|------|---------|
| **Account-level users/groups/SPs** | Already exist at account level — no migration needed. Just assign to new workspace. |
| **Workspace assignment** | Account Console → Workspaces → new workspace → Permissions. Add users/groups/SPs. Or use `databricks_mws_permission_assignment` via Terraform. |
| **Entra ID SSO** | Create new Enterprise Application (or update existing) in Azure AD. Set SAML SSO URL to new workspace URL. Update Entity ID. Download new metadata. Configure in Account Console → SSO settings. |
| **SCIM provisioning** | Update SCIM endpoint in Entra ID provisioning config to point to new workspace URL (`https://<new-ws>.azuredatabricks.net/api/2.0/preview/scim/v2/`). Generate new SCIM token on new workspace. |
| **Service Principals** | Account-level SPs: assign to new workspace via Account Console. Workspace-level SPs: recreate on new workspace. Update OAuth/PAT tokens for all SPs. |
| **PATs (Personal Access Tokens)** | All users must generate new tokens on the new workspace. Old tokens are invalid. Update all automation/CI/CD pipelines. |
| **OAuth applications** | If using custom OAuth apps: register new redirect URIs for new workspace URL. |
| **Workspace admin group** | Verify admin group is assigned to new workspace with admin permissions. |
| **IP access lists (workspace)** | Recreate workspace-level IP access lists if applicable. |

---

## 3. Migration Approach by Category

### A. Unity Catalog Migration (New Metastore)

**Sequence:**
1. Create storage credentials (managed identity or service principal for ADLS)
2. Create external locations (point to existing ADLS paths — same storage)
3. Create catalogs and schemas (same names)
4. Migrate **external tables** — metadata only, no data movement (CREATE TABLE with LOCATION)
5. Migrate **managed tables** — requires DEEP CLONE or CTAS (data copy, most time-consuming)
6. Recreate views, functions, materialized views (DDL replay)
7. Recreate volumes (external: metadata; managed: copy data + recreate)
8. Replay GRANT statements (export via SHOW GRANTS, replay on new metastore)
9. Recreate Delta Sharing shares and recipients
10. Validate row counts and data integrity

**For legacy Hive workspaces:** Recommend migrating directly to UC instead of recreating `hive_metastore`. Use this as the upgrade opportunity. Managed Hive tables require full data copy (old DBFS root). External Hive tables just need DDL recreation.

### B. Workspace Assets — Migratable via Terraform Exporter or API

| Asset | Terraform Exporter | Notes |
|-------|-------------------|-------|
| **Notebooks & workspace files** | Yes | Content exported. Revision history and comments lost. |
| **Git Repos** | Yes | Re-clone from same remote. Git credentials need re-auth. |
| **Jobs** | Yes (excl. DABs jobs) | Auto-discovers dependencies. Run history lost. Must update cluster/warehouse refs. |
| **DLT pipeline definitions** | Yes | Config only. Pipeline state/checkpoints NOT migrated — full refresh required. |
| **Cluster policies & configs** | Yes | Policy IDs change. References auto-updated in exported HCL. |
| **Instance pools** | Yes | Pool IDs change. |
| **SQL warehouses** | Yes | Warehouse IDs change. Query history lost. |
| **SQL queries & alerts** | Yes | Query IDs change. |
| **Lakeview dashboards** | Yes | JSON export/import. |
| **Legacy SQL dashboards** | Yes | Dashboard + widgets + visualizations. |
| **Model Serving endpoints** | Yes (config only) | Endpoint URLs change. Must redeploy model. Blue-green cutover for clients. |
| **Vector Search endpoints & indexes** | Yes (config only) | Must re-index from source Delta tables. Can take hours/days. |
| **Apps** | Yes (config only) | Must redeploy from source code. |
| **Secret scopes** | Yes (scopes + ACLs) | **Values redacted** — use `-export-secrets` flag or customer re-provisions. |
| **Permissions / ACLs** | Yes | Auto-emitted with parent objects. |
| **IP access lists** | Yes | Straightforward. |
| **Global init scripts** | Yes | Incremental support. |
| **Users / Groups / SPs** | Yes | With IdFed: exported as data sources (account-level). |

### C. NOT Migratable — Must Recreate Manually

| Asset | Why | What To Do |
|-------|-----|------------|
| **MLflow Experiments** | Not supported by exporter | Use **mlflow-export-import** tool |
| **MLflow Models (workspace registry)** | Not supported | Use **mlflow-export-import** tool |
| **DLT pipeline state & checkpoints** | Workspace-scoped internal state | Full refresh. Streaming DLT: coordinate offset replay. |
| **Streaming job checkpoints** | Internal state tied to workspace | Copy from ADLS if accessible, or restart from offset. |
| **Lakeflow Connect state** | Watermarks/ingestion state workspace-scoped | Recreate pipeline. May need full re-ingest. |
| **DABs-managed jobs** | Excluded from exporter | Redeploy via `databricks bundle deploy` to new workspace. |
| **Job/query run history** | Control plane internal data | Lost. Export audit logs beforehand. |
| **Notebook revision history** | Not preserved in export | Lost. Use Git for versioning. |
| **PATs & OAuth tokens** | Cannot be transferred | Users/SPs regenerate on new workspace. |
| **Git credentials** | Not exportable | Users re-authenticate on new workspace. |
| **DBFS root data** | New DBFS root provisioned | AzCopy between old and new storage accounts. |
| **UC connection passwords** | API does not return sensitive fields | Customer must re-enter credentials. |
| **Storage credential keys** | Not exported | Customer must re-provision managed identity/SP access. |

---

## 4. Infrastructure & Networking

| Component | Action | Owner |
|-----------|--------|-------|
| **Azure Private Link** | Create new PE for new CP endpoints | Customer |
| **VNet / Subnets** | Data plane VNet stays. Verify connectivity from new workspace. | Customer |
| **NSG rules** | Update if CP IPs are allowlisted | Customer |
| **DNS** | Update workspace URLs | Customer |
| **Firewall rules** | Add new CP IP ranges, remove old post-cutover | Customer |
| **Managed Identity** | New workspace identity — grant ADLS/Key Vault access | Customer |
| **Azure Key Vault** | Update access policies for new workspace identity | Customer |
| **CMK encryption** | Configure for new workspace in Account Console | Customer + Databricks |

## 5. External Integrations Checklist

Every system referencing the old workspace must be updated:

- **CI/CD** (Azure DevOps, GitHub Actions) — workspace URL, tokens
- **Terraform** — provider host, workspace ID
- **Airflow / orchestrators** — connection strings
- **BI tools** (Power BI, Tableau) — JDBC/ODBC strings, warehouse IDs
- **Azure Data Factory** — linked service config
- **SCIM / Entra ID** — SCIM endpoint, SSO URLs
- **API clients** — base URL, tokens
- **Monitoring / alerting** — workspace health endpoints
- **Event Hubs / Kafka** — if pushing to Databricks endpoints

---

## 6. Migration Phases

| Phase | Duration | Key Actions |
|-------|----------|-------------|
| **0. Discovery** | 1–2 weeks | Inventory assets per workspace. Classify complexity. Identify integrations. Size managed table data volume. |
| **1. Infrastructure** | 1 week | Create workspace + metastore in Account Console. Networking, Private Link, identity (SSO/SCIM), CMK. |
| **2. UC & Data** | 1–4 weeks | External tables (fast). Managed tables via DEEP CLONE (slow). Views, functions, grants. Validate integrity. |
| **3. Workspace Assets** | 1 week | Terraform exporter or API scripts. Notebooks, jobs, policies, warehouses, dashboards, secrets. |
| **4. Stateful Services** | 1–2 weeks | Recreate DLT, Model Serving (blue-green), Vector Search, Apps, Lakeflow Connect. MLflow export/import. |
| **5. Validation & Cutover** | 1 week | Test jobs, serving endpoints, integrations. Update DNS/SSO/SCIM. Enable new, disable old. |
| **6. Decommission** | 2–4 weeks | Dual-run grace period. Archive audit logs. Delete old workspace and Private Link endpoints. |

**Total estimated timeline: 4–8 weeks per customer** depending on data volume and complexity.

---

## 7. Risk Matrix

| Risk | Impact | Mitigation |
|------|--------|------------|
| Managed table DEEP CLONE takes too long | Extends migration window | Start largest tables first. Parallel CLONE jobs. Incremental sync for very large tables. |
| Secret values not re-provisioned | Jobs/pipelines fail | Document all scopes beforehand. Customer prepares values before cutover. |
| Streaming data gap during cutover | Data loss/duplicates | Precise cutover timing. Idempotent writes. Dual-write period. |
| External integrations missed | Production failures | Comprehensive discovery. Customer checklist. |
| Old workspace ID hardcoded in code | Runtime failures | Search repos for old workspace ID. Find-and-replace guidance. |
| Serving endpoint URL change | ML inference downtime | Blue-green deployment. Update clients before decommissioning old. |
| Permission/grant gaps | Access denied errors | Comprehensive GRANT export. Validate with test users. |

---

## 8. Customer Communication Template

**What's changing:** Your Databricks workspace control plane is moving from UK West to UK South for improved data residency and latency.

**What you need to do:**
1. Create new workspace and UC metastore (we'll guide you)
2. Run migration scripts to copy assets
3. Re-provision secrets, tokens, and credentials
4. Update external integrations (CI/CD, BI tools, SSO/SCIM, API clients)
5. Validate and cut over during agreed maintenance window

**What stays the same:** Your data in ADLS Gen2 does not move. External table data is untouched.

**What's lost:** Job/query run history, notebook revision history, existing tokens.
