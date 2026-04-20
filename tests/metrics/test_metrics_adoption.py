"""
Unit tests for project adoption score computation.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pytest

from pg_atlas.metrics.adoption import (
    aggregate_repo_downloads,
    compute_percentile_ranks,
    compute_project_adoption_scores,
    compute_repo_adoption_composites,
    downloads_by_purl_from_metadata,
)

# ---------------------------------------------------------------------------
# compute_percentile_ranks (ndarray → ndarray)
# ---------------------------------------------------------------------------


def test_percentile_min_is_zero() -> None:
    pcts = compute_percentile_ranks(np.array([0, 5, 10], dtype=np.float64))
    assert pcts[0] == 0.0


def test_percentile_max_less_than_100() -> None:
    pcts = compute_percentile_ranks(np.array([0, 5, 10], dtype=np.float64))
    assert all(v < 100.0 for v in pcts)


def test_percentile_single_element_is_zero() -> None:
    pcts = compute_percentile_ranks(np.array([42], dtype=np.float64))
    assert pcts[0] == 0.0


def test_percentile_all_same_is_zero() -> None:
    pcts = compute_percentile_ranks(np.array([5, 5, 5], dtype=np.float64))
    assert all(v == 0.0 for v in pcts)


def test_percentile_ascending_for_distinct_scores() -> None:
    pcts = compute_percentile_ranks(np.array([1, 5, 10], dtype=np.float64))
    assert pcts[0] < pcts[1] < pcts[2]


def test_percentile_values_in_range() -> None:
    pcts = compute_percentile_ranks(np.arange(10, dtype=np.float64))
    assert all(0.0 <= v < 100.0 for v in pcts)


def test_percentile_empty_returns_empty() -> None:
    pcts = compute_percentile_ranks(np.empty(0, dtype=np.float64))
    assert pcts.size == 0


# ---------------------------------------------------------------------------
# compute_repo_adoption_composites (columnar)
# ---------------------------------------------------------------------------


def test_compute_repo_adoption_composites_uses_percentiles_and_excludes_nulls() -> None:
    """
    Repo composites should average only the available signal percentiles.
    """

    composites = compute_repo_adoption_composites(
        canonical_ids=["repo-a1", "repo-a2", "repo-b1", "repo-c1"],
        downloads=[None, 100, 300, None],
        stars=[10, 20, None, None],
        forks=[2, None, 6, None],
    )

    assert composites == {
        "repo-a1": Decimal("0.00"),
        "repo-a2": Decimal("25.00"),
        "repo-b1": Decimal("50.00"),
    }


def test_compute_repo_adoption_composites_gives_ties_the_same_percentile() -> None:
    """
    Tied signal values should receive the same percentile rank.
    """

    composites = compute_repo_adoption_composites(
        canonical_ids=["repo-a", "repo-b", "repo-c"],
        downloads=[None, None, None],
        stars=[10, 10, 20],
        forks=[None, None, None],
    )

    assert composites["repo-a"] == Decimal("0.00")
    assert composites["repo-b"] == Decimal("0.00")
    assert composites["repo-c"] == Decimal("66.67")


def test_compute_project_adoption_scores_averages_child_repo_composites_only() -> None:
    """
    Project scores should ignore repos without composites and orphan repos.
    """

    repo_composites = {
        "repo-a1": Decimal("0.00"),
        "repo-a2": Decimal("25.00"),
        "repo-b1": Decimal("50.00"),
        "orphan": Decimal("75.00"),
    }

    project_scores = compute_project_adoption_scores(
        project_ids=[1, 1, 2, 3, None],
        canonical_ids=["repo-a1", "repo-a2", "repo-b1", "repo-c1", "orphan"],
        repo_composites=repo_composites,
    )

    assert project_scores == {
        1: Decimal("12.50"),
        2: Decimal("50.00"),
    }


def test_compute_repo_adoption_composites_uses_materialized_download_sum() -> None:
    """
    Repo download percentile inputs should use persisted reduced downloads values.
    """

    repo_a_downloads = aggregate_repo_downloads({"pkg:cargo/a": 100, "pkg:npm/a": 20})
    repo_b_downloads = aggregate_repo_downloads({"pkg:cargo/b": 80})
    assert repo_a_downloads == 120
    assert repo_b_downloads == 80

    composites = compute_repo_adoption_composites(
        canonical_ids=["repo-a", "repo-b"],
        downloads=[repo_a_downloads, repo_b_downloads],
        stars=[None, None],
        forks=[None, None],
    )

    assert composites["repo-a"] == Decimal("50.00")
    assert composites["repo-b"] == Decimal("0.00")


def test_downloads_by_purl_from_metadata_logs_invalid_entries(caplog: pytest.LogCaptureFixture) -> None:
    """
    Invalid metadata entries should be logged and filtered.
    """

    caplog.set_level("WARNING")
    downloads_by_purl = downloads_by_purl_from_metadata(
        {
            "adoption_downloads_by_purl": {
                "pkg:pypi/ok": 10,
                "pkg:pypi/bad": "11",
                99: 4,
            }
        },
        repo_canonical_id="pkg:github/test/repo",
    )

    assert downloads_by_purl is None
    assert "validation_errors=" in caplog.text
