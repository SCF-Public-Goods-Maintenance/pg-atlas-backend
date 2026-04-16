"""
Tests for SPDX parse helpers beyond semantic-hash behavior.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from pathlib import Path

from pg_atlas.ingestion.spdx import SpdxDependencyRelationship, parse_and_validate_spdx

FIXTURES = Path(__file__).parent / "data_fixtures"


def test_parse_and_validate_spdx_exposes_dependency_relationships_and_root_ids() -> None:
    """
    Parsed SBOMs should expose ``DEPENDS_ON`` edges and described root package ids.
    """

    raw = (FIXTURES / "graph_relationships.spdx.json").read_bytes()

    parsed = parse_and_validate_spdx(raw)

    assert parsed.root_spdx_ids == frozenset({"SPDXRef-github-test-org-test-repo-main-abc123"})
    assert parsed.dependency_relationships == (
        SpdxDependencyRelationship(
            source_spdx_id="SPDXRef-github-test-org-test-repo-main-abc123",
            target_spdx_id="SPDXRef-dep-a",
        ),
        SpdxDependencyRelationship(
            source_spdx_id="SPDXRef-dep-a",
            target_spdx_id="SPDXRef-dep-b",
        ),
    )
