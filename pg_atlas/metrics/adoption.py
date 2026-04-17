"""
Project adoption score from repo signal percentiles.

This metric uses existing repository adoption signals and computes transient
repo-level composites that are then aggregated to the project level.

Signal handling:
    - stars, forks, and downloads are ranked independently
    - each signal uses percentile ranks on a 0.0-100.0 scale
    - NULL values are excluded from the ranking pool for that signal
    - each repo composite is the mean of its available signal percentiles only

Writeback is handled separately by ``materialize_adoption`` because the schema
stores only project-level adoption scores.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from pg_atlas.metrics.criticality import compute_percentile_ranks

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


PERCENTILE_QUANTUM = Decimal("0.01")


def _quantize_score(value: Decimal) -> Decimal:
    """
    Quantize one adoption score to two decimal places.
    """

    return value.quantize(PERCENTILE_QUANTUM, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class RepoAdoptionSignals:
    """
    Snapshot the adoption signal columns needed for one repo-level computation.
    """

    canonical_id: str
    project_id: int | None
    adoption_downloads: int | None = None
    adoption_stars: int | None = None
    adoption_forks: int | None = None


def compute_repo_adoption_composites(repos: Sequence[RepoAdoptionSignals]) -> dict[str, Decimal]:
    """
    Compute transient repo-level adoption composites from available signals.

    The ranking domain is all provided ``Repo`` rows. Missing signal values are
    excluded from that signal's percentile pool rather than coerced to ``0``.
    Repos with no available signals are omitted from the result.
    """

    downloads = compute_percentile_ranks(
        {repo.canonical_id: repo.adoption_downloads for repo in repos if repo.adoption_downloads is not None}
    )
    stars = compute_percentile_ranks(
        {repo.canonical_id: repo.adoption_stars for repo in repos if repo.adoption_stars is not None}
    )
    forks = compute_percentile_ranks(
        {repo.canonical_id: repo.adoption_forks for repo in repos if repo.adoption_forks is not None}
    )

    composites: dict[str, Decimal] = {}
    for repo in repos:
        signal_percentiles: list[Decimal] = []
        for signal_scores in (downloads, stars, forks):
            percentile = signal_scores.get(repo.canonical_id)
            if percentile is not None:
                signal_percentiles.append(_quantize_score(Decimal(str(percentile))))

        if signal_percentiles:
            composites[repo.canonical_id] = _quantize_score(sum(signal_percentiles) / Decimal(len(signal_percentiles)))

    return composites


def compute_project_adoption_scores(
    repos: Sequence[RepoAdoptionSignals],
    repo_composites: Mapping[str, Decimal],
) -> dict[int, Decimal]:
    """
    Aggregate project adoption scores from child repo composites.

    Only repos with a non-null ``project_id`` and a computed repo composite
    contribute to project-level writeback.
    """

    project_values: dict[int, list[Decimal]] = {}
    for repo in repos:
        if repo.project_id is None:
            continue

        composite = repo_composites.get(repo.canonical_id)
        if composite is None:
            continue

        project_values.setdefault(repo.project_id, []).append(composite)

    return {project_id: _quantize_score(sum(values) / Decimal(len(values))) for project_id, values in project_values.items()}
