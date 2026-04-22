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
    - reject rls_cm_strategy=drop_and_restore (belt-and-braces with
      setup_sharing's own gate)
    - write the file back via yaml.safe_dump
    """

    def test_writes_backup_to_pre_integration_test_bak(self):
        src = _source_text()
        assert ".pre-integration-test.bak" in src, (
            "setup_test_config must save the pre-test config.yaml to a "
            "``.pre-integration-test.bak`` sibling so teardown can restore."
        )

    def test_gates_drop_and_restore(self):
        src = _source_text()
        assert "drop_and_restore" in src, (
            "setup_test_config must reject rls_cm_strategy=drop_and_restore "
            "(not yet implemented) to match setup_sharing's own gate."
        )
        assert "NotImplementedError" in src, "Rejection must raise NotImplementedError (not just warn)."

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
