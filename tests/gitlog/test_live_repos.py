"""
Live repository tests for the git log parser.

These tests clone real repositories and are intended for manual execution only.
Run with: ``PG_ATLAS_TEST_LIVE_REPOS=1 uv run pytest tests/gitlog/test_live_repos.py -v``

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pg_atlas.gitlog.parser import parse_repo

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_ATLAS_TEST_LIVE_REPOS"),
    reason="Set PG_ATLAS_TEST_LIVE_REPOS=1 to run live repo tests",
)


async def test_parse_real_stellar_repo(tmp_path: Path) -> None:
    """Clone a small real Stellar repo and verify non-empty result."""
    result = await parse_repo(
        repo_url="https://github.com/theahaco/scaffold-stellar",
        clone_dir=tmp_path,
        since_months=24,
        timeout=120.0,
    )
    assert result.repo_url == "https://github.com/theahaco/scaffold-stellar"
    assert result.errors == []
    # The repo should have at least some commits
    assert result.total_commits > 0
    assert len(result.contributors) > 0
    assert result.latest_commit_date is not None
