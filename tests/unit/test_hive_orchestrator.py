"""Regression tests for src/migrate/hive_orchestrator.py.

The notebook runs its main block at module import time (gated by the
``_is_notebook()`` helper which returns False outside Databricks). That
makes it awkward to drive the full flow in a pytest but we can do two
things:

1. Import-level check: the module must not call ``sys.exit`` in the
   short-circuit path (regression for the UC integration bug where
   ``sys.exit(0)`` was interpreted as FAILED by Databricks Jobs).
2. Path check: with ``dbutils`` mocked, importing the module should
   invoke ``dbutils.notebook.exit`` (not raise SystemExit) when
   ``config.include_hive`` is False.
"""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch


def _source_text() -> str:
    """Read the hive_orchestrator notebook source directly — the simplest
    regression guard that doesn't depend on import ordering."""
    import pathlib
    path = pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "hive_orchestrator.py"
    return path.read_text()


class TestHiveOrchestratorExitIdiom:
    """Source-level regression guard for the dbutils.notebook.exit fix.

    sys.exit(0) looked like a clean exit in Python, but Databricks Jobs
    treats any SystemExit raised from a notebook cell as FAILED — which
    broke every include_hive=false migrate run before PR #1.
    """

    def test_does_not_use_sys_exit(self):
        src = _source_text()
        # ``sys.exit(`` (the function call) is forbidden — use
        # dbutils.notebook.exit() for notebook short-circuits. Plain
        # ``import sys`` is allowed (used for sys.path bootstrap).
        assert "sys.exit(" not in src, (
            "hive_orchestrator must not call sys.exit() — use "
            "dbutils.notebook.exit(...) so Jobs treats the short-circuit "
            "as SUCCESS."
        )

    def test_uses_notebook_exit_for_short_circuit(self):
        src = _source_text()
        assert "dbutils.notebook.exit(" in src, (
            "hive_orchestrator must call dbutils.notebook.exit to short-"
            "circuit when include_hive=false."
        )

    def test_short_circuit_path_mentions_include_hive(self):
        """The short-circuit exit should be gated on config.include_hive
        so the orchestrator only skips when Hive scope is explicitly off."""
        src = _source_text()
        # Find the block that calls notebook.exit and verify include_hive
        # appears in its vicinity.
        idx = src.index("dbutils.notebook.exit(")
        surrounding = src[max(0, idx - 1200):idx + 200]
        assert "include_hive" in surrounding, (
            "notebook.exit() must be gated on config.include_hive — "
            "otherwise Hive migration silently becomes a no-op."
        )


class TestHiveOrchestratorShortCircuitBehavior:
    """Import-time behavior check: with mocked dbutils + config, a fresh
    import of hive_orchestrator must call notebook.exit (not raise
    SystemExit) in the skip path."""

    def test_skip_path_invokes_notebook_exit_not_sys_exit(self):
        # Build a fake dbutils that records notebook.exit calls.
        fake_dbutils = MagicMock()
        fake_dbutils.jobs.taskValues.set = MagicMock()

        # Patch the ``dbutils`` name that hive_orchestrator resolves at
        # module level via NameError check in _is_notebook. Also patch
        # MigrationConfig.from_workspace_file to return include_hive=false.
        fake_config = MagicMock()
        fake_config.include_hive = False

        # hive_orchestrator imports things at module-level. We need to
        # forcibly reload it with our mocks in scope. The _is_notebook
        # helper relies on the existence of ``dbutils`` as a NameError
        # probe, so we inject it into builtins via patch.dict + exec.
        import builtins as _builtins

        # Clear any cached module to force re-execution of the top-level
        # block.
        sys.modules.pop("migrate.hive_orchestrator", None)

        with patch.object(_builtins, "dbutils", fake_dbutils, create=True), \
             patch.object(_builtins, "spark", MagicMock(), create=True), \
             patch("common.config.MigrationConfig.from_workspace_file",
                   return_value=fake_config), \
             patch("common.tracking.TrackingManager"), \
             patch("common.auth.AuthManager"), \
             patch("common.sql_utils.find_warehouse", return_value="wh-x"), \
             patch("common.sql_utils.execute_and_poll",
                   return_value={"state": "SUCCEEDED"}):
            # Import should not raise (no SystemExit) even though the
            # short-circuit path runs.
            try:
                importlib.import_module("migrate.hive_orchestrator")
            except SystemExit as exc:  # pragma: no cover
                raise AssertionError(
                    f"hive_orchestrator raised SystemExit — it should use "
                    f"dbutils.notebook.exit() instead. Exit code: {exc.code}"
                ) from exc

        # And notebook.exit must have been called.
        fake_dbutils.notebook.exit.assert_called_once()
