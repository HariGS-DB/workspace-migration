"""Pure config-override helper extracted from ``setup_test_config.py``.

``setup_test_config.py`` is a Databricks notebook that rewrites the
workspace copy of ``config.yaml`` with per-workflow toggles. The
transformation itself is pure (dict in, dict out) — we keep it here so
it is unit-testable without a workspace / dbutils / yaml file I/O.

**Authoritative-not-additive contract.**
``apply_integration_overrides`` takes a **fresh baseline** config and
returns the override applied on top. Callers (the notebook) must
supply the pre-test baseline each invocation — NOT the previous run's
contaminated config. Specifically:

- UC integration sets ``rls_cm_strategy=drop_and_restore`` +
  ``rls_cm_maintenance_window_confirmed=true`` + ``iceberg_strategy=ddl_replay``.
- Hive integration sets ``batch_size=10`` + ``catalog_filter=integration_test_src``.
- Negative-paths runs chain four scenarios that each inject a different
  corruption (``inject_bad_spn_id`` / ``inject_unreachable_target`` /
  ``inject_bad_rls_cm``).

If a UC run's post-override config were (wrongly) fed into Hive's
override, the Hive run would silently inherit
``rls_cm_maintenance_window_confirmed=true``, a UC-specific
``catalog_filter``, or similar cross-workflow contamination. The
notebook guards against this by restoring from
``.pre-integration-test.bak`` before every invocation, so this helper
always sees the pristine baseline.
"""

from __future__ import annotations

import copy
from typing import Any


def apply_integration_overrides(
    baseline_cfg: dict,
    *,
    include_uc: bool,
    include_hive: bool,
    iceberg_strategy: str,
    rls_cm_strategy: str,
    rls_cm_maintenance_window_confirmed: bool,
    migrate_hive_dbfs_root: bool,
    hive_dbfs_target_path: str,
    batch_size_raw: str,
    catalog_filter_raw: str,
    inject_bad_spn_id: bool = False,
    inject_unreachable_target: bool = False,
    inject_bad_rls_cm: bool = False,
) -> dict[str, Any]:
    """Apply per-workflow overrides to a pristine ``config.yaml`` dict.

    Returns a new dict — ``baseline_cfg`` is deep-copied so callers can
    reuse the same baseline across multiple scenarios without
    cross-contamination.

    Raises ``ValueError`` when ``rls_cm_strategy='drop_and_restore'`` is
    requested without ``rls_cm_maintenance_window_confirmed=True`` and
    the bad-rls-cm injection is *not* active. The ``inject_bad_rls_cm``
    scenario deliberately bypasses this gate — its failure must land
    inside ``setup_sharing``'s validator, not here.
    """
    cfg = copy.deepcopy(baseline_cfg) if baseline_cfg else {}

    # The ``inject_bad_rls_cm`` scenario (X.3.4) deliberately forces
    # drop_and_restore WITHOUT the consent flag so the failure surfaces
    # inside setup_sharing's validator (not here).
    if inject_bad_rls_cm:
        rls_cm_strategy = "drop_and_restore"
        rls_cm_maintenance_window_confirmed = False
    elif (
        rls_cm_strategy.lower() == "drop_and_restore"
        and not rls_cm_maintenance_window_confirmed
    ):
        raise ValueError(
            "rls_cm_strategy='drop_and_restore' requires "
            "rls_cm_maintenance_window_confirmed=true. That path briefly strips "
            "RLS/CM on the source table during each DEEP CLONE, so it's gated "
            "behind an explicit operator confirmation."
        )

    scope = cfg.setdefault("scope", {})
    scope["include_uc"] = include_uc
    scope["include_hive"] = include_hive
    cfg["iceberg_strategy"] = iceberg_strategy
    cfg["rls_cm_strategy"] = rls_cm_strategy
    cfg["rls_cm_maintenance_window_confirmed"] = rls_cm_maintenance_window_confirmed
    cfg["migrate_hive_dbfs_root"] = migrate_hive_dbfs_root
    if hive_dbfs_target_path:
        cfg["hive_dbfs_target_path"] = hive_dbfs_target_path
    # If hive_dbfs_target_path is not provided, leave the existing
    # operator-configured value in place.
    if batch_size_raw:
        try:
            cfg["batch_size"] = max(1, int(batch_size_raw))
        except ValueError as _exc:
            raise ValueError(
                f"batch_size must be an integer, got {batch_size_raw!r}"
            ) from _exc
    if catalog_filter_raw:
        cfg["catalog_filter"] = [
            x.strip() for x in catalog_filter_raw.split(",") if x.strip()
        ]

    # --- Negative-path injections (integration X.3) ---
    # Applied AFTER the scope overrides so we are corrupting the
    # post-scope config, not the original file.
    if inject_bad_spn_id:
        cfg["spn_client_id"] = "00000000-0000-0000-0000-000000000000"
    if inject_unreachable_target:
        cfg["target_workspace_url"] = "https://adb-0000000000000000.0.azuredatabricks.net"
    if inject_bad_rls_cm:
        cfg["rls_cm_strategy"] = "drop_and_restore"
        cfg["rls_cm_maintenance_window_confirmed"] = False

    return cfg
