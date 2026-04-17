"""
Pony-factor computation helpers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Iterable

from pg_atlas.metrics.config import METRICS_CONFIG


def compute_pony_factor(
    commit_counts: Iterable[int],
    *,
    threshold_share: float = METRICS_CONFIG.pony_factor_threshold,
) -> int:
    """
    Return the minimum number of contributors responsible for the threshold share of commits.

    ``commit_counts`` may be in any order. Zero and negative values are ignored.
    A repo or project with no positive commit counts receives pony factor ``0``.
    """

    if threshold_share <= 0.0 or threshold_share > 1.0:
        raise ValueError(f"threshold_share must be within (0, 1], got {threshold_share}")

    sorted_counts = sorted((count for count in commit_counts if count > 0), reverse=True)
    total_commits = sum(sorted_counts)
    if total_commits <= 0:
        return 0

    cutoff = float(total_commits) * threshold_share
    cumulative = 0
    for contributor_count, commit_count in enumerate(sorted_counts, start=1):
        cumulative += commit_count
        if float(cumulative) >= cutoff:
            return contributor_count

    return len(sorted_counts)
