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
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

import numpy as np
from pydantic import StrictInt, TypeAdapter, ValidationError

PERCENTILE_QUANTUM = Decimal("0.01")
ADOPTION_DOWNLOADS_BY_PURL_KEY = "adoption_downloads_by_purl"
_DOWNLOADS_BY_PURL_ADAPTER: Final[TypeAdapter[dict[str, int]]] = TypeAdapter(dict[str, StrictInt])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Percentile ranking (vectorised, columnar)
# ---------------------------------------------------------------------------


def compute_percentile_ranks(values: np.ndarray) -> np.ndarray:
    """
    Convert raw scores to percentile ranks within [0.0, 100.0).

    Vectorised via ``np.searchsorted``:

        sorted  = sort(values)
        ranks   = searchsorted(sorted, values)   # count of scores < this score
        pctiles = ranks / n * 100.0

    Properties:
        - Minimum score  → 0th percentile (rank = 0).
        - Maximum score  → (n-1)/n * 100 < 100 (no element is top-ranked).
        - Ties           → all tied values receive the same (lowest) percentile.
        - Single element → 0th percentile.
        - Empty input    → empty output.

    Ecological intent: avoids the illusion that any single package is
    unconditionally "top-ranked" — the ecosystem is always the reference frame.
    """

    if values.size == 0:
        return np.empty(0, dtype=np.float64)

    sorted_values = np.sort(values)
    ranks = np.searchsorted(sorted_values, values).astype(np.float64)

    return ranks / len(values) * 100.0


# ---------------------------------------------------------------------------
# Score quantization
# ---------------------------------------------------------------------------


def _quantize_score(value: Decimal) -> Decimal:
    """
    Quantize one adoption score to two decimal places.
    """

    return value.quantize(PERCENTILE_QUANTUM, rounding=ROUND_HALF_UP)


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


def compute_repo_adoption_composites(
    canonical_ids: Sequence[str],
    downloads: Sequence[int | None],
    stars: Sequence[int | None],
    forks: Sequence[int | None],
) -> dict[str, Decimal]:
    """
    Compute transient repo-level adoption composites from columnar signal arrays.

    The ranking domain is all provided rows. Missing signal values (``None``)
    are excluded from that signal's percentile pool rather than coerced to ``0``.
    Repos with no available signals are omitted from the result.
    """

    n = len(canonical_ids)
    dl_mask = np.array([v is not None for v in downloads], dtype=np.bool_)
    st_mask = np.array([v is not None for v in stars], dtype=np.bool_)
    fk_mask = np.array([v is not None for v in forks], dtype=np.bool_)

    # Percentile arrays — NaN for rows not in pool, float for pool members.
    dl_pctiles = np.full(n, np.nan, dtype=np.float64)
    st_pctiles = np.full(n, np.nan, dtype=np.float64)
    fk_pctiles = np.full(n, np.nan, dtype=np.float64)

    if dl_mask.any():
        dl_pctiles[dl_mask] = compute_percentile_ranks(np.array([v for v in downloads if v is not None], dtype=np.float64))

    if st_mask.any():
        st_pctiles[st_mask] = compute_percentile_ranks(np.array([v for v in stars if v is not None], dtype=np.float64))

    if fk_mask.any():
        fk_pctiles[fk_mask] = compute_percentile_ranks(np.array([v for v in forks if v is not None], dtype=np.float64))

    signal_stack = np.column_stack([dl_pctiles, st_pctiles, fk_pctiles])

    composites: dict[str, Decimal] = {}
    for i in range(n):
        row = signal_stack[i]
        valid = row[~np.isnan(row)]
        if valid.size == 0:
            continue

        mean_val: float = valid.sum() / valid.size
        mean = _quantize_score(Decimal(str(mean_val)))
        composites[canonical_ids[i]] = mean

    return composites


def compute_project_adoption_scores(
    project_ids: Sequence[int | None],
    canonical_ids: Sequence[str],
    repo_composites: Mapping[str, Decimal],
) -> dict[int, Decimal]:
    """
    Aggregate project adoption scores from child repo composites.

    Only repos with a non-null ``project_id`` and a computed repo composite
    contribute to project-level writeback.
    """

    project_values: dict[int, list[Decimal]] = {}
    for i, cid in enumerate(canonical_ids):
        pid = project_ids[i]
        if pid is None:
            continue

        composite = repo_composites.get(cid)
        if composite is None:
            continue

        project_values.setdefault(pid, []).append(composite)

    return {pid: _quantize_score(sum(values) / Decimal(len(values))) for pid, values in project_values.items()}
