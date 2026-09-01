# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Repository automation should cover every maintained dependency surface."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_dependabot_covers_each_dependency_ecosystem_without_pr_flooding() -> None:
    config = yaml.safe_load((ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8"))

    assert config["version"] == 2
    updates = {(entry["package-ecosystem"], entry["directory"]): entry for entry in config["updates"]}
    assert set(updates) == {
        ("pip", "/"),
        ("npm", "/clients/typescript"),
        ("docker", "/"),
        ("github-actions", "/"),
    }

    for entry in updates.values():
        assert entry["schedule"] == {"interval": "monthly"}
        assert entry["open-pull-requests-limit"] == 5
        groups = entry["groups"]
        assert len(groups) == 1
        group = next(iter(groups.values()))
        assert group == {"patterns": ["*"], "update-types": ["minor", "patch"]}

    assert updates[("pip", "/")]["versioning-strategy"] == "increase-if-necessary"
    assert updates[("npm", "/clients/typescript")]["versioning-strategy"] == "increase-if-necessary"
