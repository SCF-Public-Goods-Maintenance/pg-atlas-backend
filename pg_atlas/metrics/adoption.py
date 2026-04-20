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

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

from pydantic import StrictInt, TypeAdapter, ValidationError

from pg_atlas.metrics.criticality import compute_percentile_ranks

PERCENTILE_QUANTUM = Decimal("0.01")
ADOPTION_DOWNLOADS_BY_PURL_KEY = "adoption_downloads_by_purl"
_DOWNLOADS_BY_PURL_ADAPTER: Final[TypeAdapter[dict[str, int]]] = TypeAdapter(dict[str, StrictInt])

logger = logging.getLogger(__name__)


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


def aggregate_repo_downloads(
    adoption_downloads_by_purl: Mapping[str, int] | None,
) -> int | None:
    """
    Resolve one repo's effective downloads from per-PURL metadata only.

    Materialization persists this sum into ``Repo.adoption_downloads`` so
    percentile ranking runs on reduced scalar values.
    """

    if not adoption_downloads_by_purl:
        return None

    return sum(adoption_downloads_by_purl.values())


def downloads_by_purl_from_metadata(
    repo_metadata: Mapping[str, object] | None,
    *,
    repo_canonical_id: str | None = None,
) -> dict[str, int] | None:
    """
    Extract and validate the per-PURL downloads map from repo metadata.

    Validation is strict (string keys + integer values). Invalid shapes are
    skipped and logged instead of silently ignored.
    """

    if repo_metadata is None:
        return None

    raw_downloads_by_purl = repo_metadata.get(ADOPTION_DOWNLOADS_BY_PURL_KEY)
    if raw_downloads_by_purl is None:
        return None

    try:
        parsed_downloads_by_purl = _DOWNLOADS_BY_PURL_ADAPTER.validate_python(raw_downloads_by_purl)
    except ValidationError as exc:
        repo_id = repo_canonical_id or "<unknown>"
        logger.error(f"Invalid {ADOPTION_DOWNLOADS_BY_PURL_KEY} for repo {repo_id}: validation_errors={exc.error_count()}")
        return None

    return parsed_downloads_by_purl if parsed_downloads_by_purl else None


def merge_download_into_repo_metadata(
    repo_metadata: Mapping[str, object] | None,
    package_purl: str,
    downloads: int,
    *,
    repo_canonical_id: str | None = None,
) -> dict[str, object]:
    """
    Upsert one package download count into repo metadata.

    The ``adoption_downloads_by_purl`` map is the crawler write target; scalar
    reduction happens later during adoption materialization.

    Existing metadata entries are preserved when valid. Invalid existing map
    shapes are discarded with logging by ``downloads_by_purl_from_metadata``.
    """

    metadata: dict[str, object] = dict(repo_metadata or {})
    existing_by_purl = downloads_by_purl_from_metadata(metadata, repo_canonical_id=repo_canonical_id) or {}
    existing_by_purl[package_purl] = downloads
    metadata[ADOPTION_DOWNLOADS_BY_PURL_KEY] = existing_by_purl

    return metadata


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
