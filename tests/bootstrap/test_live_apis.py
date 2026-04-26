"""
Live API integration tests for bootstrap tasks.

Validates that real API response structures match our parsing expectations.
Skipped by default — enable with ``PG_ATLAS_TEST_LIVE_APIS=1``.

These tests are the early warning system for API changes in GitHub.
TODO: add live tests for other APIs used in the bootstrap workflow.
They make real HTTP requests and do NOT write to any database.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import os

import pytest

from pg_atlas.procrastinate import github

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_ATLAS_TEST_LIVE_APIS"),
    reason="Set PG_ATLAS_TEST_LIVE_APIS=1 to run live API tests",
)

# ---------------------------------------------------------------------------
# GitHub live tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ["owner", "repo_name", "expected_refs"],
    [
        (
            "Soneso",
            "stellar_flutter_sdk",
            [
                github.PackageReference("DART", "stellar_flutter_sdk"),
                github.PackageReference("RUBYGEMS", "stellar_flutter_sdk"),
            ],
        ),
        ("SCF-Public-Goods-Maintenance", "pg-atlas-ts-sdk", [github.PackageReference("NPM", "@pg-atlas/data-sdk")]),
    ],
)
def test_detect_packages_from_repo_resolves_published_packages(
    owner: str, repo_name: str, expected_refs: list[github.PackageReference]
) -> None:
    """Package detection should read manifests listed by GraphQL and parse package names."""

    package_refs = github.detect_packages_from_repo(owner, repo_name)

    assert len(package_refs) == len(expected_refs)
    for i, ref in enumerate(package_refs):
        assert ref.system == expected_refs[i].system
        assert ref.name == expected_refs[i].name
