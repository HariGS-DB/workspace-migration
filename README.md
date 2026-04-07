# Azure UK South Control Plane Migration Analysis

Analysis of workspaces with data plane in **UK South** and control plane in **UK West**,
in preparation for introducing a dedicated control plane for UK South.

## Key Findings (Q4 2025: Oct-Dec 2025)

- **1,222 paying accounts** / **2,075 total active accounts** (including zero-consumption)
- **7,076 paying workspaces** / **12,139 total active workspaces**
- **56.9M DBUs** / **$15.2M** consumption in Q4 2025
- **56% UC adoption** by dollars ($8.5M UC vs $6.7M legacy)
- **12.6M UC tables** registered (4.5M managed, 2.4M external, 1.1M foreign)

## Files

| File | Description |
|------|-------------|
| `queries/01_active_workspaces.sql` | Active workspaces with Q4 consumption |
| `queries/02_product_breakdown.sql` | Product type and SKU breakdown |
| `queries/03_uc_vs_legacy.sql` | Unity Catalog vs legacy Hive split |
| `queries/04_table_type_breakdown.sql` | Managed vs external table counts |
| `data/` | CSV exports of query results |
