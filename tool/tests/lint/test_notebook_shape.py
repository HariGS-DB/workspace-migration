"""Lint checks on Databricks notebook source files.

These are file-level shape assertions that catch structural bugs before they
hit a cluster. Specifically:

1. No indented ``# COMMAND ----------`` markers — when placed inside a function
   body, Databricks treats them as cell boundaries and breaks variable scope.
2. Every ``dbutils.widgets.get(name)`` has a matching ``dbutils.widgets.text(name, ...)``
   earlier in the file (or the file delegates to ``MigrationConfig.from_job_params``
   which declares widgets with defaults).
3. Files that import ``from common...`` include a ``sys.path`` bootstrap that
   adds the bundle's ``src/`` directory — required because DAB doesn't put
   ``src/`` on the Python path at notebook runtime.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

TOOL_ROOT = Path(__file__).resolve().parent.parent.parent

# Notebook source files are those starting with "# Databricks notebook source".
NOTEBOOK_DIRS = [
    TOOL_ROOT / "src" / "pre_check",
    TOOL_ROOT / "src" / "discovery",
    TOOL_ROOT / "src" / "migrate",
    TOOL_ROOT / "tests" / "integration",
]


def _discover_notebooks() -> list[Path]:
    files: list[Path] = []
    for d in NOTEBOOK_DIRS:
        if not d.exists():
            continue
        for p in d.glob("*.py"):
            if p.name == "__init__.py":
                continue
            first_line = p.read_text().splitlines()[0] if p.read_text() else ""
            if first_line.strip().startswith("# Databricks notebook source"):
                files.append(p)
    return sorted(files)


NOTEBOOKS = _discover_notebooks()


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: str(p.relative_to(TOOL_ROOT)))
def test_no_indented_command_markers(nb: Path) -> None:
    """``# COMMAND ----------`` must sit at column 0. Indented markers split
    cells mid-function, breaking variable scope (surfaced as NameError at runtime).
    """
    bad: list[tuple[int, str]] = []
    for i, line in enumerate(nb.read_text().splitlines(), 1):
        if re.match(r"^\s+# COMMAND -+\s*$", line):
            bad.append((i, line))
    assert not bad, (
        f"{nb.relative_to(TOOL_ROOT)} has indented '# COMMAND ----------' markers: "
        f"{bad}. These are treated as cell boundaries by Databricks and break "
        f"function-scoped variables."
    )


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: str(p.relative_to(TOOL_ROOT)))
def test_widgets_declared_before_use(nb: Path) -> None:
    """Every dbutils.widgets.get(name) call must be preceded by a
    dbutils.widgets.text(name, ...) call in the same file, OR the file must
    delegate to MigrationConfig.from_job_params (which declares widgets with
    defaults).
    """
    source = nb.read_text()
    if "MigrationConfig.from_job_params" in source:
        return  # delegates to from_job_params which handles declaration
    tree = ast.parse(source)

    declared: set[str] = set()
    undeclared_gets: list[tuple[int, str]] = []

    class Walker(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
            # Match dbutils.widgets.text("name", ...) and dbutils.widgets.get("name")
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Attribute)
                and isinstance(func.value.value, ast.Name)
                and func.value.value.id == "dbutils"
                and func.value.attr == "widgets"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                name = node.args[0].value
                if func.attr == "text":
                    declared.add(name)
                elif func.attr == "get" and name not in declared:
                    undeclared_gets.append((node.lineno, name))
            self.generic_visit(node)

    Walker().visit(tree)
    assert not undeclared_gets, (
        f"{nb.relative_to(TOOL_ROOT)} calls dbutils.widgets.get() for names not "
        f"previously declared with dbutils.widgets.text(): {undeclared_gets}. "
        f"Add dbutils.widgets.text(name, default) before .get()."
    )


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: str(p.relative_to(TOOL_ROOT)))
def test_sys_path_bootstrap_when_importing_common(nb: Path) -> None:
    """If a notebook imports from the ``common`` package, it must include a
    sys.path bootstrap that adds the bundle's ``src/`` directory. Without it,
    the notebook errors with ModuleNotFoundError at runtime.
    """
    source = nb.read_text()
    if not re.search(r"^\s*from\s+common[\s\.]", source, flags=re.MULTILINE):
        return  # no common imports, no bootstrap needed

    # The bootstrap resolves the DAB files root and appends "/src"
    has_bootstrap = "/files/src" in source and "sys.path" in source
    assert has_bootstrap, (
        f"{nb.relative_to(TOOL_ROOT)} imports from `common` but lacks the "
        f"sys.path bootstrap. Add at the top:\n"
        f"  _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()\n"
        f"  _src = '/Workspace' + _nb.split('/files/')[0] + '/files/src'\n"
        f"  if _src not in sys.path: sys.path.insert(0, _src)"
    )
