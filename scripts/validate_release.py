# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Fail closed when a Git tag does not describe a prepared Python release."""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
TAG = re.compile(r"v(?P<version>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))")


def _package_version() -> str:
    tree = ast.parse((ROOT / "samsarix_chat_engine" / "__init__.py").read_text(encoding="utf-8"))
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in statement.targets
        ):
            value = ast.literal_eval(statement.value)
            if isinstance(value, str):
                return value
    raise ValueError("samsarix_chat_engine.__version__ is missing or not a string literal")


def _service_versions() -> set[str]:
    tree = ast.parse((ROOT / "samsarix_chat_engine" / "app.py").read_text(encoding="utf-8"))
    versions: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "FastAPI":
            for keyword in node.keywords:
                if keyword.arg == "version" and isinstance(keyword.value, ast.Constant):
                    versions.add(keyword.value.value)
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "version"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                ):
                    versions.add(value.value)
    if not versions or not all(isinstance(version, str) for version in versions):
        raise ValueError("service version literals are missing or invalid")
    return versions


def validate(tag: str) -> str:
    match = TAG.fullmatch(tag)
    if match is None:
        raise ValueError("release tag must use canonical vMAJOR.MINOR.PATCH syntax")
    version = match.group("version")
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project["version"] != version or _package_version() != version or _service_versions() != {version}:
        raise ValueError("tag, project metadata, package version, and service versions must match exactly")

    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    if re.search(rf"^## {re.escape(version)} — \d{{4}}-\d{{2}}-\d{{2}}$", changelog, re.MULTILINE) is None:
        raise ValueError("changelog has no dated section for the tagged version")
    unreleased = re.search(r"^## Unreleased\s*$\n(?P<body>.*?)(?=^## |\Z)", changelog, re.MULTILINE | re.DOTALL)
    if unreleased is not None and unreleased.group("body").strip():
        raise ValueError("move every Unreleased changelog entry into the dated release before tagging")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    try:
        version = validate(args.tag)
    except ValueError as exc:
        parser.error(str(exc))
    print(f"validated release v{version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
