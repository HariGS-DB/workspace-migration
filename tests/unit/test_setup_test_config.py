"""Unit tests for tests/integration/setup_test_config.py — the notebook
that rewrites the workspace copy of config.yaml per-workflow and backs
up the pre-test version for teardown to restore.

The notebook runs its main block at import (similar to
hive_orchestrator), so we use source-level regression checks plus a
scoped-import behavior test with mocked dbutils/config/os/shutil.
"""

from __future__ import annotations

import pathlib


def _source_text() -> str:
    path = pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "setup_test_config.py"
    return path.read_text()


class TestSetupTestConfigSourceGuards:
    """Source-level checks — the notebook must:
    - back up config.yaml before overriding (so teardown can restore)
    - gate rls_cm_strategy=drop_and_restore on the
      rls_cm_maintenance_window_confirmed flag (belt-and-braces with
      setup_sharing's own gate)
    - write the file back via yaml.safe_dump
    """

    def test_writes_backup_to_pre_integration_test_bak(self):
        src = _source_text()
        assert ".pre-integration-test.bak" in src, (
            "setup_test_config must save the pre-test config.yaml to a "
            "``.pre-integration-test.bak`` sibling so teardown can restore."
        )

    def test_gates_drop_and_restore_on_maintenance_window_flag(self):
        """drop_and_restore briefly strips RLS/CM on source during each
        DEEP CLONE, so setup_sharing already refuses it unless
        ``rls_cm_maintenance_window_confirmed=true``. We mirror that gate
        here so misconfiguration fails fast at config-setup time (before
        we do any side-effecting I/O)."""
        src = _source_text()
        assert "drop_and_restore" in src, (
            "setup_test_config must reference rls_cm_strategy=drop_and_restore "
            "(we accept it conditionally, matching setup_sharing's gate)."
        )
        assert "rls_cm_maintenance_window_confirmed" in src, (
            "setup_test_config must surface the rls_cm_maintenance_window_"
            "confirmed flag as a task widget — otherwise operators can't "
            "opt into drop_and_restore from the integration workflow."
        )
        # The gate must actually raise, not just log, when drop_and_restore
        # is set without maintenance-window confirmation.
        assert "raise ValueError" in src or "raise NotImplementedError" in src, (
            "setup_test_config must raise when drop_and_restore is set "
            "without maintenance-window confirmation — logging alone lets "
            "the run continue and strip RLS/CM without operator consent."
        )

    def test_uses_yaml_safe_dump(self):
        src = _source_text()
        assert "yaml.safe_dump" in src, (
            "Must use yaml.safe_dump (not yaml.dump) to avoid !!python/"
            "object tags when writing customer-visible config."
        )

    def test_backup_before_override(self):
        """Backup must precede the file write — otherwise a mid-notebook
        crash leaves config.yaml corrupted with no way to restore."""
        src = _source_text()
        backup_idx = src.index(".pre-integration-test.bak")
        override_idx = src.index("yaml.safe_dump")
        assert backup_idx < override_idx, "Backup copy must happen BEFORE the file is overwritten."

    def test_preserves_existing_hive_dbfs_target_path_when_empty_param(self):
        """Env-specific fields like hive_dbfs_target_path are operator-
        configured once post-deploy. The notebook must only override them
        when the corresponding task param is non-empty, preserving the
        operator's value when the param is the default empty string."""
        src = _source_text()
        assert "if hive_dbfs_target_path:" in src, (
            "hive_dbfs_target_path must be conditionally overridden — "
            "an empty task param should preserve the operator's value."
        )


class TestNegativePathInjections:
    """Integration X.3: source-level checks for the negative-path
    injection widgets. They must:

    - Declare all three injection widgets (bad_spn_id / unreachable_target /
      bad_rls_cm) with default "false" so normal UC / Hive integration
      workflows are unaffected.
    - Apply each injection AFTER the scope overrides so the corrupted
      config is what downstream tasks read.
    - Force ``rls_cm_maintenance_window_confirmed = False`` when the bad
      rls_cm strategy is injected — the whole point of the scenario is
      the missing consent, so we can't trust whatever value happened to
      be in config.yaml.
    - Bypass the top-of-notebook ``drop_and_restore`` consent gate ONLY
      for the bad_rls_cm injection — other callers hitting
      drop_and_restore without the confirmation flag must still trip
      the ValueError guard so operators don't accidentally set it.
    """

    def test_declares_three_injection_widgets(self):
        src = _source_text()
        for name in ("inject_bad_spn_id", "inject_unreachable_target", "inject_bad_rls_cm"):
            assert f'dbutils.widgets.text("{name}", "false")' in src, (
                f"Must declare injection widget {name!r} with default 'false' "
                "so the widget is optional and normal workflows are unaffected."
            )

    def test_bad_spn_injection_overwrites_spn_client_id(self):
        src = _source_text()
        assert 'cfg["spn_client_id"] = "00000000-0000-0000-0000-000000000000"' in src, (
            "bad_spn injection must overwrite spn_client_id with a "
            "well-formed-but-wrong UUID so the SDK accepts the shape but "
            "token exchange fails at pre_check."
        )

    def test_unreachable_target_injection_overwrites_target_url(self):
        src = _source_text()
        assert 'cfg["target_workspace_url"]' in src, (
            "unreachable_target injection must overwrite target_workspace_url."
        )

    def test_bad_rls_cm_forces_maintenance_window_false(self):
        """The scenario's whole premise is the missing consent — we must
        NOT trust whatever value config.yaml happened to have."""
        src = _source_text()
        assert 'cfg["rls_cm_maintenance_window_confirmed"] = False' in src, (
            "bad_rls_cm injection must force "
            "rls_cm_maintenance_window_confirmed=False so the setup_sharing "
            "validator fails for the expected reason."
        )
        assert 'cfg["rls_cm_strategy"] = "drop_and_restore"' in src

    def test_bad_rls_cm_bypasses_consent_gate(self):
        """The normal drop_and_restore gate raises ValueError at the top
        of the notebook when ``rls_cm_maintenance_window_confirmed`` is
        false. For bad_rls_cm the failure must land in setup_sharing's
        validator (deeper in the workflow), not here — so the gate must
        be an elif under ``inject_bad_rls_cm``."""
        src = _source_text()
        assert "if inject_bad_rls_cm:" in src
        # New contract (post-PR #22): consent-gated ValueError, not
        # NotImplementedError. The elif must carry both the strategy
        # check AND the consent-flag check so injection path short-
        # circuits before either fires.
        assert "elif (" in src
        assert 'rls_cm_strategy.lower() == "drop_and_restore"' in src
        assert "not rls_cm_maintenance_window_confirmed" in src
        assert "raise ValueError" in src
        assert "raise NotImplementedError" not in src, (
            "drop_and_restore is now implemented (PR #22); the old "
            "NotImplementedError stub must be gone."
        )
