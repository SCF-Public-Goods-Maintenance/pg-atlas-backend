"""
Unit tests for pg_atlas.metrics.pony_factor.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from pg_atlas.metrics.pony_factor import compute_pony_factor


def test_single_contributor_has_pony_factor_one() -> None:
    assert compute_pony_factor([100]) == 1


def test_exact_half_of_commits_counts_toward_threshold() -> None:
    assert compute_pony_factor([5, 5]) == 1


def test_equal_ten_contributors_require_five_people() -> None:
    assert compute_pony_factor([1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) == 5


def test_majority_contributor_can_determine_pony_factor_alone() -> None:
    assert compute_pony_factor([5, 60, 5, 5, 5, 5, 5, 5, 5, 5]) == 1


def test_empty_or_zero_commit_distributions_return_zero() -> None:
    assert compute_pony_factor([]) == 0
    assert compute_pony_factor([0, 0, 0]) == 0
