# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Release publication must stay version-bound, least privilege, and attestable."""

from __future__ import annotations

from pathlib import Path

import pytest

import scripts.validate_release as release
from scripts.validate_release import validate

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("tag", ["0.12.0", "v0.12", "v0.12.0-rc1", "v01.2.3", "latest", "v1.2.3/extra"])
def test_release_tag_must_be_canonical_semver(tag: str) -> None:
    with pytest.raises(ValueError, match="canonical"):
        validate(tag)


def test_current_tree_cannot_be_mislabeled_as_the_prior_release() -> None:
    with pytest.raises(ValueError, match="Unreleased"):
        validate("v0.12.0")


def release_tree(tmp_path: Path, *, project: str = "0.13.0", package: str = "0.13.0", service: str = "0.13.0") -> Path:
    root = tmp_path / "release"
    package_dir = root / "samsarix_chat_engine"
    package_dir.mkdir(parents=True)
    (root / "pyproject.toml").write_text(f'[project]\nname = "example"\nversion = "{project}"\n', encoding="utf-8")
    (package_dir / "__init__.py").write_text(f'__version__ = "{package}"\n', encoding="utf-8")
    (package_dir / "app.py").write_text(
        f'def create():\n    app = FastAPI(version="{service}")\n    return {{"version": "{service}"}}\n',
        encoding="utf-8",
    )
    (root / "CHANGELOG.md").write_text("# Changelog\n\n## 0.13.0 — 2026-08-31\n\n- Ready.\n", encoding="utf-8")
    return root


def test_release_validator_accepts_one_consistent_prepared_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(release, "ROOT", release_tree(tmp_path))
    assert validate("v0.13.0") == "0.13.0"


@pytest.mark.parametrize("surface", ["project", "package", "service"])
def test_release_validator_rejects_any_version_surface_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, surface: str
) -> None:
    versions = {"project": "0.13.0", "package": "0.13.0", "service": "0.13.0"}
    versions[surface] = "0.12.9"
    monkeypatch.setattr(release, "ROOT", release_tree(tmp_path, **versions))
    with pytest.raises(ValueError, match="must match exactly"):
        validate("v0.13.0")


def test_unreleased_content_at_end_of_file_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = release_tree(tmp_path)
    with (root / "CHANGELOG.md").open("a", encoding="utf-8") as changelog:
        changelog.write("\n## Unreleased\n\n- Not released.\n")
    monkeypatch.setattr(release, "ROOT", root)
    with pytest.raises(ValueError, match="Unreleased"):
        validate("v0.13.0")


@pytest.mark.parametrize(
    "metadata",
    [
        '[project]\nname = "example"\nversion = "0.13.0"\nversion = "0.13.0"\n',
        "[project]\nname = \"example\"\nversion = '0.13.0'\n",
        '[build-system]\nversion = "0.13.0"\n',
    ],
)
def test_project_version_metadata_must_be_one_unambiguous_literal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, metadata: str
) -> None:
    root = release_tree(tmp_path)
    (root / "pyproject.toml").write_text(metadata, encoding="utf-8")
    monkeypatch.setattr(release, "ROOT", root)
    with pytest.raises(ValueError, match="version"):
        validate("v0.13.0")


def test_release_workflow_is_tag_only_pinned_and_least_privilege() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert 'tags:\n      - "v*.*.*"' in workflow
    assert "pull_request:" not in workflow and "workflow_dispatch:" not in workflow
    assert "contents: read" in workflow
    assert workflow.count("contents: write") == 1
    assert workflow.count("id-token: write") == 1
    assert workflow.count("attestations: write") == 1
    assert "persist-credentials: false" in workflow
    assert "--verify-tag" in workflow and "--prerelease" in workflow
    assert '"pip==26.2.1"' in workflow and '"setuptools==84.0.0"' in workflow
    assert '"build==1.6.0"' in workflow and '"wheel==0.47.0"' in workflow and '"twine==6.2.0"' in workflow
    assert "python -m build --no-isolation" in workflow
    assert "actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6" in workflow
    assert "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" in workflow
    assert "@v" not in workflow
    assert "pypi" not in workflow.casefold() and "npm publish" not in workflow.casefold()
