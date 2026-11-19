"""HPCsizer must run with nothing but the standard library.

It is used on login nodes with the system python3, by people who have not
installed anything; the previous version failed there on ``import click``.
"""

import ast
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SOURCES = sorted((REPO / "lib").glob("*.py")) + [REPO / "bin" / "hpg", REPO / "bin" / "monitor.py"]
OPTIONAL = {"lib/plotter.py": {"matplotlib"}}


def imported_modules(path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


@pytest.mark.skipif(not hasattr(sys, "stdlib_module_names"), reason="needs Python 3.10+")
@pytest.mark.parametrize("path", SOURCES, ids=lambda p: str(p.relative_to(REPO)))
def test_only_the_standard_library_is_imported(path):
    allowed = (
        set(sys.stdlib_module_names) | {"lib"} | OPTIONAL.get(str(path.relative_to(REPO)), set())
    )
    assert imported_modules(path) - allowed == set()


def test_no_installer_files_remain():
    """There is no install step: bin/ goes on PATH."""
    assert not (REPO / "setup.py").exists() and not (REPO / "requirements.txt").exists()


def test_every_module_is_importable_on_its_own():
    import importlib

    for path in sorted((REPO / "lib").glob("*.py")):
        if path.name != "__init__.py":
            importlib.import_module("lib." + path.stem)
