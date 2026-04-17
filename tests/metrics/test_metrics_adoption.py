"""
Unit tests for project adoption score computation.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from decimal import Decimal

from pg_atlas.metrics.adoption import (
    RepoAdoptionSignals,
    compute_project_adoption_scores,
    compute_repo_adoption_composites,
)


def test_compute_repo_adoption_composites_uses_percentiles_and_excludes_nulls() -> None:
    """
    Repo composites should average only the available signal percentiles.
    """

    repos = [
        RepoAdoptionSignals(canonical_id="repo-a1", project_id=1, adoption_stars=10, adoption_forks=2),
        RepoAdoptionSignals(canonical_id="repo-a2", project_id=1, adoption_stars=20, adoption_downloads=100),
        RepoAdoptionSignals(canonical_id="repo-b1", project_id=2, adoption_forks=6, adoption_downloads=300),
        RepoAdoptionSignals(canonical_id="repo-c1", project_id=3),
    ]

    composites = compute_repo_adoption_composites(repos)

    assert composites == {
        "repo-a1": Decimal("0.00"),
        "repo-a2": Decimal("25.00"),
        "repo-b1": Decimal("50.00"),
    }


def test_compute_repo_adoption_composites_gives_ties_the_same_percentile() -> None:
    """
    Tied signal values should receive the same percentile rank.
    """

    repos = [
        RepoAdoptionSignals(canonical_id="repo-a", project_id=1, adoption_stars=10),
        RepoAdoptionSignals(canonical_id="repo-b", project_id=1, adoption_stars=10),
        RepoAdoptionSignals(canonical_id="repo-c", project_id=1, adoption_stars=20),
    ]

    composites = compute_repo_adoption_composites(repos)

    assert composites["repo-a"] == Decimal("0.00")
    assert composites["repo-b"] == Decimal("0.00")
    assert composites["repo-c"] == Decimal("66.67")


def test_compute_project_adoption_scores_averages_child_repo_composites_only() -> None:
    """
    Project scores should ignore repos without composites and orphan repos.
    """

    repos = [
        RepoAdoptionSignals(canonical_id="repo-a1", project_id=1),
        RepoAdoptionSignals(canonical_id="repo-a2", project_id=1),
        RepoAdoptionSignals(canonical_id="repo-b1", project_id=2),
        RepoAdoptionSignals(canonical_id="repo-c1", project_id=3),
        RepoAdoptionSignals(canonical_id="orphan", project_id=None),
    ]
    repo_composites = {
        "repo-a1": Decimal("0.00"),
        "repo-a2": Decimal("25.00"),
        "repo-b1": Decimal("50.00"),
        "orphan": Decimal("75.00"),
    }

    project_scores = compute_project_adoption_scores(repos, repo_composites)

    assert project_scores == {
        1: Decimal("12.50"),
        2: Decimal("50.00"),
    }
